# -*- coding: utf-8 -*-

from tqdm import tqdm
import numpy as np
import networkx as nx
import sqlite3 as s3
from datetime import datetime, timedelta
import pickle

conn = s3.connect('CTROT')
c = conn.cursor()

start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925

folder_n = "mn_networks/"

for current_date in tqdm([start_date + timedelta(x) for x in range(total_days-7)]):
    g = nx.DiGraph()
    for c_date in [current_date + timedelta(i) for i in range(7)]:
        c.execute("SELECT * FROM mentions WHERE created_at = '"+c_date.strftime("%Y-%m-%d")+"'")
        for user in c.fetchall():
            g.add_edge(user[1],user[2])
    g.remove_edges_from(g.selfloop_edges())
    isolates = [x for x in nx.isolates(g)]
    g.remove_nodes_from(isolates)

    with open(folder_n+current_date.strftime("%Y-%m-%d")+"_reg.pickle",'wb') as current_file:
        pickle.dump(g,current_file)
        
    #Normalization part
    # if nx.number_of_nodes(g)>4:
    #     try:
    #         g = nx.double_edge_swap(g, nswap=2*nx.number_of_nodes(g), max_tries=1000*nx.number_of_nodes(g))
    #     except Exception as inst:
    #         print(inst)

    # with open(folder_n+current_date.strftime("%Y-%m-%d")+"_norm.pickle",'wb') as current_file:
    #     pickle.dump(g,current_file)
