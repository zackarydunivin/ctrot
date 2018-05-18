from tqdm import tqdm
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import pickle

#Needs egolist.txt and accounts_ts.pickle

print("Prepping data")
start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925
folder = "k_timelines_n_rt/"
t = [start_date + timedelta(x) for x in range(total_days)]
t = [datetime.strftime(i,"%Y-%m-%d") for i in t]

with open("egolist.txt",'r') as f:
    egolist = [thing.replace("\n","") for thing in f.readlines()]

with open("accounts_ts.pickle",'rb') as f:
    accounts_ts = pickle.load(f)

#=========================

print("Sifting folders...")
summed_data = {}
for filename in tqdm(os.listdir(os.getcwd()+"/"+folder)):
    with open(folder+filename,'r') as f:
        total_at_day = 0
        for line in f:
            one_line = line.replace("\n","").split(",")
            if one_line[0] in egolist:
                total_at_day += int(one_line[2])
    summed_data[filename.replace(".txt","")] = total_at_day
num_accounts = {t[x]:accounts_ts[x] for x in range(len(t))}

print("Normalizing...")
norm_ts = []
for date in t:
    norm_ts.append( float(summed_data[date])/num_accounts[date] ) 

print("Pushing normalized ts to file...")
with open("norm_ts_rt.pickle",'wb') as f:
    pickle.dump(norm_ts,f)
    
print("Done.")
