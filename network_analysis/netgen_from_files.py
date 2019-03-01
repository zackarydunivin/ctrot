import multiprocessing as mp
import networkx as nx
import os
import pickle
from datetime import datetime, timedelta
import csv

from tqdm import tqdm

start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925
binsize = 7

source_folder = "" #"/N/dc2/scratch/vmwong/"
rtfiles_folder = "us_wn_timelines_rt/"
graphs_folder = "networks_folder/"

def networkMake(datelist):
    print("%s started..."%filename)    
    g = nx.Graph()
    for date in datelist:
        fn = date.strftime("%Y-%m-%d")+".csv"
        #Open csv
        with open(rtfiles_folder + fn,'rb') as f:
            data = csv.reader(f,delimiter=', ')
            next(data) #Skip the first line
            for row in data:
                g.add_edge( tuple(row) )
    
    #Pickle the network
    date = datelist[0].strftime("%Y-%m-%d") + ".pickle"
    with open(graphs_folder + date, 'wb' ) as f:
        pickle.dump(g,f)
    print("%s finished!"%filename)

if __name__ == "__main__":
    pool = mp.Pool(processes = 8)
    
    #List of dates
    list_of_dates = []
    for current_date in [start_date + timedelta(x) for x in range(total_days-binsize)]:
        list_of_dates.append( [current_date + timedelta(i) for i in range(binsize)] )
    
    multiple_results = [pool.apply_async(networkMake,(datelist,)) for datelist in list_of_dates]
