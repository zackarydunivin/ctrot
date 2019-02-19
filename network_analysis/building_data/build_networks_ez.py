# -*- coding: utf-8 -*-

import numpy as np
import networkx as nx
import sqlite3 as s3
from datetime import datetime, timedelta
import pickle
import sys
from tqdm import tqdm
import os

conn = s3.connect('CTROT_mid')
c = conn.cursor()

start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925
binsize = int(sys.argv[1])

folder_n = "/N/dc2/scratch/vmwong/rt_networks/"

l1 = set([]); l2 = set([]);
for x in os.listdir(folder_n):
    thing = x.split("_")[0]
    if thing not in l1:
        l1.add(thing)
    else:
        l2.add(thing)
done_dates = l1 & l2

for current_date in tqdm([start_date + timedelta(x) for x in range(total_days-binsize)]):
    print(current_date)
    if current_date.strftime("%Y-%m-%d") in done_dates:
        continue
    g = nx.Graph()
    for c_date in [current_date + timedelta(i) for i in range(binsize)]:
        c.execute("SELECT * FROM retweets WHERE created_at='"+c_date.strftime("%Y-%m-%d 00:00:00")+"'")
        for user in c.fetchall():
            g.add_edge(user[1],user[2])
    g.remove_edges_from(g.selfloop_edges())
    isolates = [x for x in nx.isolates(g)]
    g.remove_nodes_from(isolates)

    with open(folder_n+current_date.strftime("%Y-%m-%d")+"_reg.pickle",'wb') as current_file:
        pickle.dump(g,current_file)
        
    #Normalization part
    if nx.number_of_nodes(g)>4:
        try:
            g = nx.double_edge_swap(g, nswap=2*nx.number_of_nodes(g), max_tries=1000*nx.number_of_nodes(g))
        except Exception as inst:
            print(inst)

    with open(folder_n+current_date.strftime("%Y-%m-%d")+"_norm.pickle",'wb') as current_file:
        pickle.dump(g,current_file)


#strftime('%Y-%m-%d',created_at)='"+c_date.strftime("%Y-%m-%d 00:00:00")+"'" )
