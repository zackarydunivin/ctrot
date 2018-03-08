# -*- coding: utf-8 -*-
import os
import sys
import glob
#import gzip
#import time
#import json
import cymysql as mysql
#from datetime import datetime, timedelta
#import emoji 
import argparse

from write_db_functs import *

###############################################
# CONNECTIONS
###############################################

print "Starting..."

# parse command line...
parser = argparse.ArgumentParser(description='Command line for manual_twitter_scrape.py.')
# main function arguments...
parser.add_argument('-timelines_glob', type=str, required=True)
parser.add_argument('-user', type=str, default='root')
parser.add_argument('-password', type=str)
parser.add_argument('-db', type=str)
parser.add_argument('-host', type=str, default='rdc04.uits.iu.edu')
parser.add_argument('-port', type=int, default=3148)
parser.add_argument('-only_new', type=bool, default=1)

opts = parser.parse_args()

print 'Connected to database as user %s on host %s...' % (opts.user,opts.host)
conn = mysql.connect(host=opts.host, user=opts.user, passwd=opts.password, db=opts.db, port=opts.port,charset='utf8')

c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS tweets
                 (tweet_id varchar(30), user_id varchar(12), text VARCHAR(500) CHARACTER SET utf8mb4, source text,
                 created_at int, created_at_string char(19), in_reply_to_status_id varchar(30),  
                 in_reply_to_user_id varchar(12), longitude int, latitude int, 
                 place text, quoted_status_id varchar(30), quoted_status text,
                 retweeted_status text, quote_count int, reply_count int, 
                 retweet_count int, favorite_count int, user_mentions text,
                 urls text, possibly_sensitive bit, filter_level varchar(12), lang text)''') 

c.execute('''CREATE TABLE IF NOT EXISTS retweets
                 (tweet_id varchar(30), outgoing_user_id varchar(22), incoming_user_id varchar(22),
                 created_at_string date, retweeted_status_id varchar(30), is_quote int)''') 
    
c.execute('''CREATE TABLE IF NOT EXISTS mentions
                 (tweet_id varchar(30), outgoing_user_id varchar(22), incoming_user_id varchar(22),
                 created_at_string date, is_reply int, in_reply_to_status_id varchar(30))''') 

createUniqueIndices(c)

conn.commit()

###############################################
# PROCESSING
###############################################

f = open('gz_already_done.txt','a+')
gz_already_done = [thing.replace("\n","") for thing in f.readlines()]
tweet_fields = ['tweet_id','user_id','text','source','created_at','created_at_string','in_reply_to_status_id','in_reply_to_user_id','longitude','latitude','place','quoted_status_id','quoted_status','retweeted_status','quote_count','reply_count','retweet_count','favorite_count','user_mentions','urls','possibly_sensitive','filter_level','lang']
retweet_fields = ['tweet_id','outgoing_user_id','incoming_user_id','created_at_string','retweeted_status_id','is_quote']
mention_fields = ['tweet_id','outgoing_user_id','incoming_user_id','created_at_string','is_reply','in_reply_to_status_id']

#The queue
tweetlist = []
retweetlist = []
mentionlist = []
new_tweet_ids_not_bulk_inserted_yet = []
bulk_size = 100000
only_new = True

for gz in sorted(glob.glob(opts.timelines_glob)):
    print(gz)
    #Skip files you've already done
    if gz in gz_already_done:
        continue
    count = 0
    for tweet in gzip_timeline_generator(gz):
        if not tweet:
            continue
        tweet = json.loads( tweet.decode("utf8").replace(",\n","") )

        #Tweets
        tweetlist.append(parseTweet(tweet))
        new_tweet_ids_not_bulk_inserted_yet.append(tweet['id'])
        #Retweets
        if 'retweeted_status' in tweet:
            retweetlist.append(parseRetweets(tweet))
        #Mentions
        if 'user_mentions' in tweet['entities']:
            mentionlist.append(parseMentions(tweet))

        if len(tweetlist)>bulk_size:
            tweetlist, new_tweet_ids_not_bulk_inserted_yet = bulkInsert(c,conn,tweetlist,'tweets',tweet_fields)
        if len(retweetlist)>bulk_size:
            retweetlist, new_retweet_ids_not_bulk_inserted_yet = bulkInsert(c,conn,retweetlist,'retweets',retweet_fields)
        if len(mentionlist)>bulk_size:
            mentionlist, new_mention_ids_not_bulk_inserted_yet = bulkInsert(c,conn,mentionlist,'mentions',mention_fields)

        #Counting
        count += 1
        if count%bulk_size==0:
            print count
    #Final extras:
    bulkInsert(c,conn,tweetlist,'tweets',tweet_fields)
    bulkInsert(c,conn,retweetlist,'retweets',retweet_fields)
    bulkInsert(c,conn,mentionlist,'mentions',mention_fields)
    conn.commit()
    f.write(gz+"\n")
print "Done."
f.close()

createIndices()
