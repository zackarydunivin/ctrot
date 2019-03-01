import multiprocessing as mp
import networkx as nx
import os
import pickle
from datetime import datetime, timedelta
import csv
import sys

start_date = datetime.strptime("2009-11-18 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-18 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2871
binsize = int(sys.argv[1])

source_folder = "/N/dc2/scratch/vmwong/"
rtfiles_folder = "us_wn_timelines_rt/"
graphs_folder = "networks_folder_%i/"%binsize

if graphs_folder not in os.listdir(source_folder):
    os.makedirs(source_folder+graphs_folder)

def networkMake(datelist):
    date_fn = datelist[0].strftime("%Y-%m-%d") + ".pickle"
    print("%s started..."%date_fn)    
    
    g = nx.Graph()
    for date in datelist:
        fn = date.strftime("%Y-%m-%d")+".csv"
        #Open csv
        with open(source_folder + rtfiles_folder + fn,'r') as f:
            data = csv.reader(f,delimiter=',')
            next(data) #Skip the first line
            for row in data:
                g.add_edge(row[0],row[1])
    
    #Pickle the network
    with open(source_folder + graphs_folder + date_fn, 'wb' ) as f:
        pickle.dump(g,f)
    
    print("%s finished!"%date_fn)

if __name__ == "__main__":
    pool = mp.Pool(processes = 8)
    
    #List of dates
    list_of_dates = []
    for current_date in [start_date + timedelta(x) for x in range(total_days-binsize)]:
        list_of_dates.append( [current_date + timedelta(i) for i in range(binsize)] )
    
    multiple_results = [pool.apply_async(networkMake,(datelist,)) for datelist in list_of_dates]