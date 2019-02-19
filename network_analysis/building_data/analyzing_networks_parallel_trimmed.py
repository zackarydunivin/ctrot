import networkx as nx
from networkx.algorithms import community as comm1
import community as comm2
from tqdm import tqdm
import numpy as np
from matplotlib import pyplot as plt
import pickle
import igraph as ig
import pandas as pd
import os

from collections import defaultdict #Used to transpose dictionary
from itertools import combinations

import multiprocessing as mp

networks_folder = "rt_networks/"
nodestats_folder = "rt_nodestats/"

seed_nodes = [27522964, 42786325, 52352820, 72931184, 75736238, 87229781, 90657826, 114374226, 152852932, 154891961, 209693451, 271397818, 322027737, 402181258, 433462889, 1457805708, 2167525794, 2231109295, 2349347329, 2442888666, 2490088512, 2888660047, 2915187060, 2995401932, 3012158891, 3234613430, 3999537573, 813914393788481540, 817163123182465024, 832319306541178881, 850408392925499392, 857482409012547584, 894682675893735426, 903046544919732224]

def shortestPath2Seeds(source_seed,seed_nodes,g):
    test_minimum_path = []
    #Remove edges from seed nodes
    seed_nodes_in_graph = [x for x in seed_nodes if x in g]
    if len(seed_nodes_in_graph)==0:
        return -2
    for remaining_seed in seed_nodes_in_graph:
        removed_seed_edges = g.edges([x for x in seed_nodes if x!= remaining_seed])
        h = nx.DiGraph(g)
        h.remove_edges_from(removed_seed_edges)
        if remaining_seed in h and nx.has_path(h,source_seed,remaining_seed):
            test_minimum_path.append( len(nx.shortest_path(h,source_seed,remaining_seed)) )
        #g.add_edges_from(removed_seed_edges)
    if test_minimum_path==[]:
        return -1
    else: 
        return min(test_minimum_path)
    
def transposeDict(original_dict,items=False):
    result = defaultdict(list)
    items_l = original_dict.items() if not items else original_dict
    for k,v in items_l:
        result[v].append(k)
    result = dict(result)
    return result

def incrementWeights(partition_transpose,g_temp):
    for comm in partition_transpose:
        for e1,e2 in combinations(partition_transpose[comm],2):
            if g_temp.has_edge(e1,e2):
                g_temp[e1][e2]['weight'] += 1
    return g_temp

def incrementAdjacency(partition_transpose, adj_dict):
    for comm in partition_transpose:
        for e1,e2 in combinations(partition_transpose[comm],2):
            if (e1,e2) not in adj_dict:
                adj_dict[(e1,e2)] = 0
            adj_dict[(e1,e2)] += 1
    return adj_dict

def savedf(df):
    with open(nodestats_folder+filename,'wb') as f:
        df.to_csv()
    print("%s finished!"%filename)

def networkCalc(filename):
    with open(networks_folder+filename,'rb') as f:
        g = pickle.load(f)
    k_core = nx.core_number(g)
#     shortest_paths_2_seeds = {}
#     for node in tqdm(g):
#         shortest_paths_2_seeds[node] = shortestPath2Seeds(node,seed_nodes,g)
    
    #Creating igraph
    g_ig_lookup_table = {}
    counter = 0
    for node in g:
        g_ig_lookup_table[node] = counter
        counter += 1
    g_ig = ig.Graph(len(g), [(g_ig_lookup_table[x],g_ig_lookup_table[y]) for x,y in list(g.edges()) if y!='None'])
    g_ig_lookup_table_rev = transposeDict(g_ig_lookup_table)
    
    #Creating consensus cluster
    g_consensus_cluster = nx.Graph(g)
    nx.set_edge_attributes(g_consensus_cluster,name='weight',values=0)
    #Creating consensus matrix
    adj_dict = {}
    
    #Communities over time - Using consensus clustering
    #Louvain community
    partition_lou = comm2.best_partition(nx.Graph(g))
    partition_lou_transpose = transposeDict(partition_lou)
    
    #Infomap
    partition_info = g_ig.community_infomap(trials=20)
    partition_memberships = zip([g_ig.vs[x].index for x in range(g_ig.vcount())], partition_info.membership)
    partition_info_transpose = transposeDict(partition_memberships,items=True)
    partition_info_transpose_relabel = {}
    for thing in partition_info_transpose:
        partition_info_transpose_relabel[thing] = set([g_ig_lookup_table_rev[x][0] for x in partition_info_transpose[thing]])
    
    #Label propogation method
    #comm1.label_propagation_communities(g) #networkx implementation, doesn't seem to work
    partition_label = g_ig.community_label_propagation()
    partition_memberships = zip([g_ig.vs[x].index for x in range(g_ig.vcount())], partition_label.membership)
    partition_label_transpose = transposeDict(partition_memberships,items=True)
    partition_label_transpose_relabel = {}
    for thing in partition_label_transpose:
        partition_label_transpose_relabel[thing] = set([g_ig_lookup_table_rev[x][0] for x in partition_label_transpose[thing]])
    
    if True:
        g_consensus_cluster = incrementWeights(partition_lou_transpose,g_consensus_cluster)
        g_consensus_cluster = incrementWeights(partition_info_transpose_relabel,g_consensus_cluster)
        g_consensus_cluster = incrementWeights(partition_label_transpose_relabel,g_consensus_cluster)
        partition_f = comm2.best_partition(g_consensus_cluster, weight='weight')

    if False:
        adj_dict = incrementAdjacency(partition_lou_transpose,adj_dict)
        adj_dict = incrementAdjacency(partition_info_transpose_relabel,adj_dict)
        adj_dict = incrementAdjacency(partition_label_transpose_relabel,adj_dict)
        g_num_nodes = nx.number_of_nodes(g)
        adj_matrix = np.zeros((g_num_nodes,g_num_nodes))
        for k,v in adj_dict.items():
            adj_matrix[k[0],k[1]] += v
        partition_f_adj = comm2.best_partition(nx.from_numpy_matrix(adj_matrix))
           
    df = pd.DataFrame([k_core,partition_f]).T.rename(index=str,columns={0:"k_value",1:"Community_graph"}) #,2:"Community_adj"})
    
    savedf(df)

if __name__ == "__main__":
    pool = mp.Pool(processes = 32)
    
    multiple_results = [pool.apply_async(networkCalc,(filename,)) for filename in os.listdir(networks_folder) if "_reg" in filename]
