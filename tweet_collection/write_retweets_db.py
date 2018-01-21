# -*- coding: utf-8 -*-
import os
import sys
import glob
import gzip
import time
from tqdm import tqdm
import json
import pymysql as mysql
from datetime import datetime, timedelta
import tempfile




def upsert_to_db(table,d,fields,only_new):
    # ordered values
    values = [d[k] for k in fields]
    query = 'INSERT INTO %s VALUES (' % table + ','.join(['%s']*len(d)) + ')'
    if only_new:
        #c.execute(query,list(d.values()))
        c.execute(query,values)
    else:
        query += ' ON DUPLICATE KEY UPDATE ' + ', '.join([k + '=%s' for k in d])
        #c.execute(query,list(d.values())+list(d.values()))
        c.execute(query,values+values)

def upsert_mentions(mentions,general_dict,fields,is_reply,only_new):
    if not mentions or mentions == [None]:
        return

    mentions_dict = general_dict.copy()
    mentions_dict['is_reply'] = int(is_reply)
    for m in mentions:
        # we already got this one
        if not is_reply and reply[0] == m:
            break
        # if is mention in reply will still have in_reply_to_status_id
        # unless next two lines are uncommennted
        #elif not is_reply:
        #    mention_dict['in_reply_to_status_id'] = None
        
        mentions_dict['incoming_user_id'] = m
        upsert_to_db('mentions',mentions_dict,fields,only_new)

def connection_kwargs(opts):
    kwarg_keys = ['db',
                  'password',
                  'user',
                  'host',
                  'port',]
    kwargs = {}
    d = vars(opts)
    for k in kwarg_keys:
        if d[k]:
            kwargs[k] = d[k]

    return kwargs

def tmp_cnf_connect(opts):
    # create the cnf file as a temporary file and then pass it to mysql.connect  
    cnf_string = """[client]
ssl-mode = disabled
default-character-set = utf8mb4

[mysql]
default-character-set = utf8mb4

[mysqld]
# utf8mb4 encoding
character-set-client-handshake = FALSE
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci
"""
    cnf_bytes = cnf_string.encode('utf-8')

    cnf = tempfile.NamedTemporaryFile(delete=True)
    cnf.write(cnf_bytes)
    cnf.seek(0)
    cnf.read()
    conn = mysql.connect(read_default_file=cnf.name,**connection_kwargs(opts))
    cnf.close()
    return conn

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for manual_twitter_scrape.py.')
    # main function arguments...
    parser.add_argument('-timelines_glob', type=str, required=True)
    parser.add_argument('-cnf_path', type=str)
    parser.add_argument('-user', type=str, default='root')
    parser.add_argument('-password', type=str)
    parser.add_argument('-db', type=str, required=True)
    parser.add_argument('-host', type=str, default='rdc04.uits.iu.edu')
    parser.add_argument('-port', type=int, default=3148)
    parser.add_argument('-no_retweets', action='store_true')
    parser.add_argument('-no_mentions', action='store_true')
    parser.add_argument('-only_new', action='store_true')
    parser.add_argument('-no_write', action='store_true')


    opts = parser.parse_args()
    
    sys.stdout.write('Connected to database as user %s on host %s...\n' % (opts.user,opts.host))
    if opts.cnf_path:
        conn = mysql.connect(db = opts.db, read_default_file=opts.cnf_path)
    else:
        conn = tmp_cnf_connect(opts)
        
    sys.stdout.write('OK.\n')
    sys.stdout.flush()

    c = conn.cursor()

    #Create tables
    #User that retweeted, user that was retweeted, number of times this happened for that date, other data?

    c.execute('''CREATE TABLE IF NOT EXISTS retweets
                 (tweet_id varchar(30), outgoing_user_id varchar(22), incoming_user_id varchar(22),
                 created_at int, created_at_string char(19), retweeted_status_id varchar(30), is_quote int)''') 
    
    c.execute('''CREATE TABLE IF NOT EXISTS mentions
                 (tweet_id varchar(30), outgoing_user_id varchar(22), incoming_user_id varchar(22),
                 created_at int, created_at_string char(19), is_reply int, in_reply_to_status_id varchar(30))''') 
    
    retweet_fields = ['tweet_id',
             'outgoing_user_id',
             'incoming_user_id',
             'created_at',
             'created_at_string',
             'retweeted_status_id',
             'is_quote']
    
    mention_fields = ['tweet_id',
             'outgoing_user_id',
             'incoming_user_id',
             'created_at',
             'created_at_string',
             'is_reply',
             'in_reply_to_status_id']


    try:
        c.execute("CREATE UNIQUE INDEX id_index ON retweets (id);")
        c.execute("CREATE UNIQUE INDEX id_index ON mentions (id);")
    except:
        pass


    indices = [('outgoing_user_id_index','outgoing_user_id'),
               ('incoming_user_id_index','incoming_user_id')]

    for i in indices:
        try:
            c.execute("CREATE INDEX %s ON retweets (%s);" % i)
            c.execute("CREATE INDEX %s ON mentions (%s);" % i)
        except:
            pass

    if not opts.no_write:
        conn.commit()

    def gzip_timeline_generator(filename):
        with gzip.open(gz, 'rb') as f:
            for line in f:
                yield line
    def parse_mentions(tweet):
        # all user ids mentioned in this tweet
        mentions = []
        if not tweet['entities']['user_mentions']:
            return None
        else:
            for m in tweet['entities']['user_mentions']:
                mentions.append(m['id'])
        return mentions

    sys.stdout.write('Parsing %d files and writing the enclosed retweets/mentions to database.\n' % len(glob.glob(opts.timelines_glob)))
    sys.stdout.flush()

    for gz in sorted(glob.glob(opts.timelines_glob)):
        count = 0
        for tweet in tqdm(gzip_timeline_generator(gz)):
            if not tweet:
                continue
            tweet = tweet.decode("utf8").replace(",\n","")
            tweet = json.loads(tweet)
            
            if opts.only_new:
                c.execute('select tweet_id from retweets where tweet_id=%s limit 1;',tweet['id'])
                search = c.fetchone()
                if search:
                    continue
                c.execute('select tweet_id from mentions where tweet_id=%s limit 1;',tweet['id'])
                search = c.fetchone()
                if search:
                    continue

            
            date = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')

            general_dict = {}            
            general_dict['tweet_id'] = tweet['id']
            general_dict['outgoing_user_id'] = (tweet['user']['id'])
            general_dict['created_at'] = (int(date.timestamp()))
            general_dict['created_at_string'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            
            mention_dict = general_dict.copy()
            mentions = parse_mentions(tweet)
            reply = [tweet['in_reply_to_user_id']]
            mention_dict['in_reply_to_status_id'] = tweet['in_reply_to_status_id']
            
            # retweet
            try:
                retweet = tweet['retweeted_status']['user']['id']
                retweet_dict = general_dict.copy()
                retweet_dict['incoming_user_id'] = retweet
                retweet_dict['retweeted_status_id'] = tweet['retweeted_status']['id']
                retweet_dict['is_quote'] = 0
                upsert_to_db('retweets',retweet_dict,retweet_fields,opts.only_new)
            except KeyError:
                pass
            # quote
            try:
                if not opts.no_retweets:
                    retweet = tweet['quoted_status']['user']['id']
                    retweet_dict = general_dict.copy()
                    retweet_dict['incoming_user_id'] = retweet
                    retweet_dict['retweeted_status_id'] = tweet['quoted_status']['id']
                    retweet_dict['is_quote'] = 1
                    upsert_to_db('retweets',retweet_dict,retweet_fields,opts.only_new)
            except KeyError:
                pass

            if not opts.no_mentions:
                # reply
                upsert_mentions(reply,mention_dict,mention_fields,True,opts.only_new)
                # mentions
                upsert_mentions(mentions,mention_dict,mention_fields,False,opts.only_new)

            if not opts.no_write:
                conn.commit()