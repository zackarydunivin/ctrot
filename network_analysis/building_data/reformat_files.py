import pickle
from tqdm import tqdm
import numpy as np
from datetime import datetime, timedelta
import os
import sys

start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00")
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925

with open("userlist_z.txt",'r') as f:
    egolist = [thing.replace("\n","") for thing in f.readlines()]

folder = "k_timelines/"
folder_n = "k_timelines_n/"

#This will be very memory-intensive
data = {}
t = [start_date + timedelta(x) for x in range(total_days)]
t = [datetime.strftime(i,"%Y-%m-%d") for i in t]

for filename in os.listdir(os.getcwd()+"/"+folder):
    print filename
    with open(folder+filename,'r') as f:
        for line in f:
            one_line = line.replace("\n","").split(",")
            if one_line[0] in egolist:
                if one_line[0] not in data:
                    data[one_line[0]] = np.zeros(total_days)
                data[one_line[0]][t.index(one_line[1])] = int(one_line[2])

print "Pickling..."
### Poop the junk out to a file
with open("k_values.pickle","w") as f:
    pickle.dump(data,f)
print "Done."
