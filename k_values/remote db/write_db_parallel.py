import time
import numpy as np
import sys

import networkx as nx
import cymysql as mysql
import argparse
from datetime import datetime, timedelta

import multiprocessing as mp

###################
## CONNECTION
###################

print "Starting..."

# parse command line...
parser = argparse.ArgumentParser(description='Command line for manual_twitter_write_db_parallel.py.')
# main function arguments...
parser.add_argument('-user', type=str, default='root')
parser.add_argument('-password', type=str)
parser.add_argument('-db', type=str)
parser.add_argument('-host', type=str, default='rdc04.uits.iu.edu')
parser.add_argument('-port', type=int, default=3148)

opts = parser.parse_args()

print 'Connected to database as user %s on host %s...' % (opts.user,opts.host)
connection_details = (opts.host, opts.user, opts.password, opts.db, opts.port)

conn = mysql.connect(host=connection_details[0], user=connection_details[1], passwd=connection_details[2], db=connection_details[3], port=connection_details[4],charset='utf8')

c = conn.cursor()

###################
## BASICS
###################

#Get the dates
#THIS USED TO MANUALLY PULL THE DATES FROM THE RETWEETS TABLE,
#BUT THAT WAS COMPUTATIONALLY EXPENSIVE, SO I DECIDED TO PLACE
#THEM HERE MANUALLY. CONFIRM AGAINST RETWEETS TABLE.
start_date = datetime.strptime("2009-10-21 00:00:00","%Y-%m-%d 00:00:00") 
end_date = datetime.strptime("2017-10-24 00:00:00","%Y-%m-%d 00:00:00")
total_days = 2925

# c.execute("DROP TABLE k_values_p;")
# print c.fetchall()
# c.execute("DROP TABLE normalized_k_p;")
# print c.fetchall()

c.execute('''CREATE TABLE IF NOT EXISTS k_values_p2
             (user text, date DATE, k_value int)''')
# c.execute('''CREATE TABLE IF NOT EXISTS normalized_k_p2
#              (user text, date DATE, k_value int)''')

# c.execute('''CREATE INDEX kp_user_index ON k_values_p(user);''')
# c.execute('''CREATE INDEX kp_date_index ON k_values_p(date);''')
# c.execute('''CREATE INDEX kn_user_index ON normalized_k_p(user);''')
# c.execute('''CREATE INDEX kn_date_index ON normalized_k_p(date);''')

conn.close()

###################
## FUNCTIONS
###################
class Networker(mp.Process):
    
    def __init__(self, task_queue, result_queue):
        mp.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
    
    def run(self):
        proc_name = self.name
        while True:
            next_date = self.task_queue.get()
            if next_date==None:
                print '%s: Exiting' % proc_name
                self.task_queue.task_done()
                break
            answer = next_date()
            self.task_queue.task_done()
            self.result_queue.put(answer)
        return

class createNetwork(object):

    def __init__(self,start_date, days, connection_details):
        self.start_date = start_date
        self.days = days
        if type(self.start_date)==str:
            self.start_date = datetime.strptime(start_date,"%Y-%m-%d 00:00:00")
        self.connection_details = connection_details

    def __call__(self):
        g = nx.Graph()
        conn = mysql.connect(host=self.connection_details[0], user=self.connection_details[1], passwd=self.connection_details[2], db=self.connection_details[3], port=self.connection_details[4],charset='utf8')
        c = conn.cursor()
        for current_date in (self.start_date + timedelta(x) for x in range(self.days)):
            looking = True
            tries = 0
            while looking:
                try:
                    c.execute("SELECT * FROM k_retweets WHERE created_at='"+current_date.strftime("%Y-%m-%d 00:00:00")+"'" )
                    looking = False
                except Exception as inst:
                    time.sleep(0.1)
                    tries+=1
                    print "Selecting: %i tries..."%tries,
                    print inst
                    sys.stdout.flush()
                    continue
            for user in c.fetchall():
                g.add_edge(user[1],user[2])
        g.remove_edges_from(g.selfloop_edges())

        #Then calculate k-values and output them into the database 
        k_values_doc = nx.core_number(g)
        k_values = [(user,current_date,k_values_doc[user]) for user in k_values_doc]

        #Then calculate normalization and output it to a file
        # g_norm = nx.Graph(g)
        # if nx.number_of_nodes(g_norm)>4:
        #     g_norm = nx.double_edge_swap(g_norm, nswap=2*nx.number_of_nodes(g_norm), max_tries=float("inf"))
        # k_values_doc = nx.core_number(g_norm)
        # k_values_norm = [(user,current_date,k_values_doc[user]) for user in k_values_doc]
        
        self.upload(k_values,'k_values_p2',c)
#        self.upload(k_values_norm,'normalized_k_p2',c)
        conn.commit()
        conn.close()
        print "1",
        sys.stdout.flush()
        return 
    
    def upload(self,data,db,c):
        inserting = True
        tries = 0
        while inserting:
            try:
                c.executemany('INSERT INTO '+db+' VALUES (%s,%s,%s)', data)
                inserting = False
            except Exception as inst:
                time.sleep(0.1)
                tries += 1
                print "Uploading: %i tries..."%tries,
                print inst
                sys.stdout.flush()
                continue

###################
## MAIN
###################

if __name__ == '__main__':
    test_date_num = total_days
    t = [start_date + timedelta(x) for x in range(test_date_num)]
    t = [current_date.strftime("%Y-%m-%d 00:00:00") for current_date in t]
    
    # Establish communication queues
    dates = mp.JoinableQueue()
    results = mp.Queue()
    
    # Start consumers
    num_networkers = 8 #mp.cpu_count()
    print 'Creating %d network creators' % num_networkers
    networkers = [ Networker(dates, results)
                  for i in xrange(num_networkers) ]
    for w in networkers:
        w.daemon = True
        w.start()
    
    # Enqueue jobs
    num_jobs = test_date_num
    for date in t:
        dates.put(createNetwork(date, 7, connection_details))
    
    # Add a poison pill for each consumer
    for i in xrange(num_networkers):
        dates.put(None)

    # Wait for all of the tasks to finish
    dates.join()
    
    # Start printing results
    while num_networkers:
        result = results.get()
        print 'Result:', result
        num_networkers -= 1
