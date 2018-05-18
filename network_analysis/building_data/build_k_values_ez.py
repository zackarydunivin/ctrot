# -*- coding: utf-8 -*-

from tqdm import tqdm
import numpy as np
import networkx as nx
import sqlite3 as s3
from datetime import datetime, timedelta

conn = s3.connect('CTROT')
c = conn.cursor()

start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925

folder_n = "k_timelines_n_mn/"

for current_date in tqdm([start_date + timedelta(x) for x in range(total_days-7)]):
    g = nx.Graph()
    for c_date in [current_date + timedelta(i) for i in range(7)]:
        c.execute("SELECT * FROM mentions WHERE strftime('%Y-%m-%d',created_at)='"+c_date.strftime("%Y-%m-%d 00:00:00")+"'" )
        for user in c.fetchall():
            g.add_edge(user[1],user[2])
    g.remove_edges_from(g.selfloop_edges())
    isolates = [x for x in nx.isolates(g)]
    g.remove_nodes_from(isolates)

    #Normalization part
    if nx.number_of_nodes(g)>4:
        try:
            g = nx.double_edge_swap(g, nswap=2*nx.number_of_nodes(g), max_tries=1000*nx.number_of_nodes(g))
        except Exception as inst:
            print(inst)

    #Then calculate k-values and output them into the database 
    k_values_doc = nx.core_number(g)
    k_values = [(user,current_date.strftime("%Y-%m-%d"),k_values_doc[user]) for user in k_values_doc]
    
    with open(folder_n+current_date.strftime("%Y-%m-%d")+".txt",'w') as current_file:
        for line in k_values:
            current_file.write(str(line[0])+","+line[1]+","+str(line[2])+"\n")
