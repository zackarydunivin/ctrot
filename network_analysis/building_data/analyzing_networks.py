import networkx as nx
import os
import pickle
from datetime import datetime, timedelta
import csv

start_date = datetime.strptime("2009-11-18 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-18 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2871
binsize = 7

source_folder = "/N/dc2/scratch/vmwong/"
graphs_folder = "networks_folder_%i/"%binsize
stats_folder = "nodestats_folder_%i/"%binsize

if stats_folder not in [x+"/" for x in os.listdir(source_folder)]:
    os.makedirs(source_folder+stats_folder)
    
#Get userlist here
with open("egolist.txt",'r') as f:
    userlist = [x.replace("\n","") for x in f.readlines()]
userfiles_created = set([])

list_of_dates = sorted(os.listdir(source_folder + graphs_folder))

for date in list_of_dates:
    date_str = date.replace(".pickle","")
    date_fn = date
    
    with open(source_folder + graphs_folder + date_fn,'rb') as f:
        g = pickle.load(f)
        
    k_decomp = nx.core_number(g)
    
    for node in k_decomp:
        if node in userlist:
            if node not in userfiles_created:
                with open(source_folder+stats_folder+node+".txt",'w') as f:
                    f.write(date_str + ", " + str(k_decomp[node]) + "\n")
                    userfiles_created.add(node)
            else:
                with open(source_folder+stats_folder+node+".txt",'a') as f:
                    f.write(date_str + ", " + str(k_decomp[node]) + "\n")
