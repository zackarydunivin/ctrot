from tqdm import tqdm
import numpy as np
from datetime import datetime, timedelta
import os
import sys
import pickle
import multiprocessing as mp

#Needs egolist.txt and accounts_ts.pickle

class Worker(mp.Process):
    
    def __init__(self, task_queue, result_queue):
        mp.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
    
    def run(self):
        proc_name = self.name
        while True:
            next_date = self.task_queue.get()
            if next_date==None:
                print('%s: Exiting' % proc_name)
                self.task_queue.task_done()
                break
            answer = next_date()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return

class parseFile(object):

    def __init__(self, date, egolist, accounts_ts, fol=''):
        self.filename = date+".txt"
        self.date = date
        self.egolist = egolist
        self.accounts_ts = accounts_ts
        self.folder = fol

    def __call__(self):
        print(self.filename)
        with open(self.folder+self.filename,'r') as f:
            total_at_day = 0
            for line in f:
                one_line = line.replace("\n","").split(",")
                if one_line[0] in self.egolist:
                    total_at_day += int(one_line[2])
        answer = (self.date, total_at_day)
        return answer

    
###################
## MAIN
###################

if __name__ == '__main__':
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
    
    # Establish communication queues
    dates = mp.JoinableQueue()
    results = mp.Queue()
    
    # Start consumers
    num_workers = 8 #mp.cpu_count()
    print('Creating %d workers' % num_workers)
    workers = [ Worker(dates, results) for i in range(num_workers) ]
    for w in workers:
        w.daemon = True
        w.start()
    
    # Enqueue jobs
    #num_jobs = total_days
    for date in t:
        dates.put(parseFile(date, egolist, accounts_ts,fol=folder))
    
    # Add a poison pill for each consumer
    for i in xrange(num_workers):
        dates.put(None)

    # Wait for all of the tasks to finish
    dates.join()
    results.put(None)

    print("Getting results...")
    summed_data = {}
    a_result = results.get()
    while a_result!=None:
        summed_data[a_result[0]] = a_result[1]
        a_result = results.get()
    
    norm_ts = []
    num_accounts = {t[x]:accounts_ts[x] for x in range(len(t))}
    for date in t:
        norm_ts.append( float(summed_data[date])/num_accounts[date] ) 
    
    print("Pushing normalized ts to file...")
    with open("norm_ts_mn.pickle",'wb') as f:
        pickle.dump(norm_ts,f)

    print("Done.")
