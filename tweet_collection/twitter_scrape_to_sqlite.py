import importlib
import time, json, math, os
from tqdm import tqdm
import sqlite3
import sqlite_utils as su
from twython import Twython, TwythonError, TwythonRateLimitError

# -*- coding: UTF-8 -*-

''' ### Code to scrape followers of a set of users
# Required input: .txt file of user ids (number) named 'follower#_ids.txt' where # is the desired starting degree
                  However, if inputting seeds, seed file name needs to be specified.

                my_keys.py -- A file with variables for Twitter API authorization.
                   key = 'blahblahblah123123123123'
                   secret = 'blahblahblah456456456456'
                   owner = 'your_username'
                   owner_id = 'your_userid'
                   access_token = 'blahblahblah123123123123thisoneissuperlonglikereallylonglikedamn'
                   access_token_secret = 'blahblahblah123123123123thisoneisshorter'

# Output: A file named "follower#_ids.json" where # is the target degree (1 greater than starting)
          Contains json objects with one key (account id) that returns a list of user ids that follow them.

# Notes:
        Output json is not valid input for this file.
        Degree variable is only pertinent for degree == 1 which assumes input file contains screen names and not ids.
'''


def get_followers_cursor(account_id,use_id=True,cursor=-1):
    if use_id:
        cursor = api.get_followers_ids(user_id = account_id,count=5000,cursor=next_cursor)
    else:
        cursor = api.get_followers_ids(screen_name = account_id,count=5000,cursor=next_cursor)
    return cursor

def get_followings_cursor(account_id,use_id=True,cursor=-1):
    if use_id:
        cursor = api.get_friends_ids(user_id = account_id,count=5000,cursor=next_cursor)
    else:
        cursor = api.get_friends_ids(screen_name = account_id,count=5000,cursor=next_cursor)
    return cursor

def get_followers(account_id,use_id=True):
    if use_id:
        followers = api.get_followers_ids(user_id = account_id,return_pages=True,)
    else:
        followers = api.get_followers_ids(screen_name = account_id,return_pages=True,)
    return followers

def get_followings(account_id,use_id=True):
    if use_id:
        followings = api.get_friends_ids(user_id = account_id)
    else:
        followings = api.get_friends_ids(screen_name = account_id)
    return followings


def get_user(account_id,use_id=True):
    if use_id:
        user = api.lookup_user(user_id=account)
    else:
        user = api.lookup_user(screen_name=account)
    return user

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
    parser.add_argument('-seeds_are_ids', type=bool, default=True)
    parser.add_argument('-max_followers', type=float, default=float('inf'))
    parser.add_argument('-router_id', type=str, default='WL Down 5G')
    parser.add_argument('-router_password', type=str, default=float('1234567890'))
    parser.add_argument('-degree', type=int,default=1)

    opts = parser.parse_args()

    if opts.keys_fname[-3:] == '.py':
        opts.keys_fname = opts.keys_fname[0:-3]


    keys = __import__(opts.keys_fname)

    log_file = open(opts.seeds_fname+'.log','wb')
    
    #Twython init
    api = Twython(keys.key, keys.secret, keys.access_token, keys.access_token_secret)


    with open(opts.seeds_fname,'r') as start_ids_f:
        start_ids = list(set([x.replace('\n','') for x in start_ids_f.readlines()]))
    print("%i unique accounts to search"%len(start_ids))

    # connect to the database and prepare the table
    conn = su.sqlite_connect(su.FOLLOWERS_DB_PATH)
    cur = su.sqlite_get_cursor(conn)
    su.prepare_followers_table(cur,su.FOLLOWERS_TABLE_NAME)
    count = 0
    for account in tqdm(start_ids):
        # this one isn't in the database, but we don't want it
        if not su.sqlite_check_for_user_id(cur,su.FOLLOWERS_TABLE_NAME,account):
            continue
        # this one doesn't have json for some reason, 
        #probably because it was suspended between when we collected
        # it's followers and when we added the user json
        if not cur.execute('select user_json from %s where user_id=? and user_json is not Null' % su.FOLLOWERS_TABLE_NAME, (account,)).fetchone():
            continue
        

        user_json = cur.execute('select user_json from %s where user_id=? and user_json is not Null' % su.FOLLOWERS_TABLE_NAME, (account,)).fetchone()[0]
        user = json.loads(user_json)

        if user['followers_count'] == 0:
            continue
        
        # thorough check
        # check to see if we have already entered this id into the db
        followers = cur.execute('SELECT followers from %s WHERE user_id=?' % su.FOLLOWERS_TABLE_NAME, (account,)).fetchone()
        # this one has no followers in the db, but should
        if not followers[0]:
            pass
        # comma separated values in a string
        # we got almost all the followers, we can be content that we don't need to update the user        
        #elif user['followers_count']*.95 < followers[0].count(',')+1 < user['followers_count']*1.05:
            #print("We were looking for %d and we found %d. Seems good enough to me..." %(user['followers_count'],followers[0].count(',')+1 ))
        #    continue
        elif user['followers_count']*.8 < followers[0].count(',')+1 < user['followers_count']*1.05:
            #print("We were looking for %d and we found %d. Seems good enough to me..." %(user['followers_count'],followers[0].count(',')+1 ))
            continue
        #print("We were looking for %d but we found only %d. Let's see if we can't find some more..." %(user['followers_count'],followers[0].count(',')+1 ))

        elif 10**6 < user['followers_count']:
            #print("We were looking for %d and we found %d. Seems good enough to me..." %(user['followers_count'],followers[0].count(',')+1 ))
            continue
        # if account is protected, ignore it and move on to the next one
        if user['protected'] == True:
            continue
        
        account_followers = []
        next_cursor = -1
        while next_cursor:
            try:
                #print(user['followers_count'])
                search = get_followers_cursor(account,opts.seeds_are_ids,cursor=next_cursor)
                account_followers.extend(search['ids'])
                next_cursor = search['next_cursor']
                #time.sleep(68)
                header = api.get_lastfunction_header("x-rate-limit-reset")
                waittime = float(header) - time.time()
                #print("Wait Time:", waittime/60, "minutes") #Readout of minutes remaining
                #print("Sleeping a bit")
                if waittime > 0:
                    time.sleep(waittime)
                #print("awake")
                #continue
            # too many requests
            except TwythonRateLimitError:
                print('Followers rate limit error')
                #log_file.write('%s\n' % account)
                header = api.get_lastfunction_header("x-rate-limit-reset")
                waittime = float(header) - time.time()
                time.sleep(waittime + 10) #+1 added to account for any rounding issues, as time.sleep() crashes if presented with a negative value

            except TwythonError as e:
                # no internet connection
                if e.error_code == 8:
                    # wait a minute
                    time.sleep(60*1)
                    # reconnect and try again
                    os.system('networksetup -setairportnetwork airport %s %s' % (opts.router_id, opts.router_password))
                else:
                    print(e)
                    break
            except StopIteration:
                break

        # convert list of ints into a comma separated string
        followers = [str(f) for f in account_followers]
        followers_string = ', '.join(followers)
        user_json = json.dumps(user,separators=(',',':'))
        # insert the followers
        su.sqlite_followers_insert(cur,su.FOLLOWERS_TABLE_NAME,account,followers_string,user_json,opts.degree)
        conn.commit()


    sys.stdout.write('Completed follower grab for all ids in %s'%opts.seeds_fname)