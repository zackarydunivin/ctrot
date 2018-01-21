import importlib
import time, json, math, os
from tqdm import tqdm
import sqlite3
import sqlite_utils as su
from twython import Twython, TwythonError, TwythonRateLimitError

# -*- coding: UTF-8 -*-


def get_users(account_ids):
    users = api.lookup_user(user_id=account_ids)
    return users

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for manual_twitter_scrape.py.')
    # main function arguments...
    parser.add_argument('-keys_fname', type=str, required=True)
    parser.add_argument('-seeds_fname', type=str, required=True)

    opts = parser.parse_args()

    if opts.keys_fname[-3:] == '.py':
        opts.keys_fname = opts.keys_fname[0:-3]


    keys = __import__(opts.keys_fname)


    #Twython init
    api = Twython(keys.key, keys.secret, keys.access_token, keys.access_token_secret)
    
    with open(opts.seeds_fname,'r') as start_ids_f:
        start_ids = list(set([x.replace('\n','') for x in start_ids_f.readlines()]))
    print("%i unique accounts to search"%len(start_ids))

    # connect to the database and prepare the table
    conn = su.sqlite_connect(su.FOLLOWERS_DB_PATH)
    cur = su.sqlite_get_cursor(conn)
    su.prepare_followers_table(cur,su.FOLLOWERS_TABLE_NAME)

    try:   
        cur.execute('ALTER TABLE %s ADD COLUMN user_json TEXT;' % su.FOLLOWERS_TABLE_NAME)
    except sqlite3.OperationalError:
        pass
    #ids = cur.execute('SELECT user_id FROM %s' % su.FOLLOWERS_TABLE_NAME).fetchall()
    #num_ids = cur.execute('SELECT count(*) FROM %s' % su.FOLLOWERS_TABLE_NAME).fetchone()
    #print("%i unique accounts to search" % num_ids)
    
    cur2 = su.sqlite_get_cursor(conn)

    # generator to list
    ids = [account for account in start_ids]
    batch_size = 100
    count =0
    commit_count = 0
    for batch in tqdm([ids[i:i + batch_size] for i in range(0,len(ids),batch_size)]):
        # convert list of ints into a comma separated string
        users = [str(f) for f in batch]
        users = ', '.join(users)
        try:
            users = get_users(users)
        except TwythonRateLimitError as e:
            print('Taking a short nap...')
            time.sleep(60*15)
            print('awake')
            users = get_users(batch)
        except TwythonError as e:
            print(e)
            continue
        for user in users:
            count += 1
            try:
                user_json = json.dumps(user,separators=(',',':'))
                if su.sqlite_check_for_user_id(cur,su.FOLLOWERS_TABLE_NAME,user['id']):
                    cur2.execute('UPDATE %s SET user_json=? WHERE user_id=?;' % su.FOLLOWERS_TABLE_NAME, (user_json,user['id'],)).fetchone()
                else:
                    su.sqlite_followers_insert(cur,su.FOLLOWERS_TABLE_NAME,user['id'],None,user_json,1)
                conn.commit()
                commit_count += 1
            # add the user's account information to the db    
            # too many requests
            except TwythonError as e:
                    # skip this user if their account is suspended
                    if e.error_code == 63:
                        continue
                    # or if the user does not exist
                    if e.error_code == 50:
                        continue
                    else:
                        print(inst.message)
                        continue
        print(count,commit_count)