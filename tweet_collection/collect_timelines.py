from twython import Twython, TwythonRateLimitError, TwythonError
from glob import glob
from csv import DictReader, DictWriter
import os
from datetime import datetime
import time
from tqdm import tqdm
import json
from subprocess import call

def sleep_until(ts):
    """ Sleep until the given UTC UNIX TIMESTAMP. """
    next_time = datetime.utcfromtimestamp(int(ts))
    now = datetime.utcnow()
    offset = (next_time - now).seconds
    print("Enhancing calm. Next try: {} (Currently {})".format(next_time, now))
    print("Sleeping for {}...".format(offset))
    time.sleep(offset)
    print("Continuing...")


def timeline(user_id, max_id=None):
    """ Loop over a user's timeline, starting at max_id. 
    Generator.
      
    We can get up to 15 pages. 
    This function loops up to 16 times to make the base case 
    `len(tweets) == 0` trigger."""
    for i in range(16):
        tweets = tw.get_user_timeline(user_id=user_id,
                                      max_id=max_id,
                                      count=200,
                                      trim_user=True,
                                      exclude_replies=False,
                                      include_rts=True)
        if len(tweets) > 0:  # last page should have zero results
            for tweet in tweets:
                max_id = tweet['id'] - 1
                yield tweet
        else:
            break

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
    parser.add_argument('-users_fname', type=str, required=True)
    parser.add_argument('-router_id', type=str, default='WL Down 5G')
    parser.add_argument('-router_password', type=str, default=float('1234567890'))

    opts = parser.parse_args()

    if opts.keys_fname[-3:] == '.py':
        opts.keys_fname = opts.keys_fname[0:-3]


    keys = __import__(opts.keys_fname)
    
    #Twython init
    tw = Twython(keys.key, keys.secret, keys.access_token, keys.access_token_secret)

    with open(opts.users_fname,'r') as start_ids_f:
        start_ids = list(set([x.replace('\n','') for x in start_ids_f.readlines()]))
    print("%i unique accounts to search"%len(start_ids))

    folder = 'db/timelines/' + opts.users_fname[5:]
    if not os.path.exists(folder):
        call(['mkdir', folder])


    for account in tqdm(start_ids):
        path = "%s/%s.json" % (folder,account)
        if os.path.exists(path):
            continue # skip users that have already been read.
        # max_id is used to continue a user's timeline from 
        # the last tweet read in the event that we get 
        # rate-limited in the middle of a user's timeline
        json_string = ''
        broken = False
        max_id = None
        while True:
            try:
                for tweet in timeline(account, max_id):
                    json_string += json.dumps(tweet)
                    json_string += ',\n'
                    max_id = tweet['id']
                max_id = None
                break
            except TwythonRateLimitError as e:
                print('Be right back. Taking a nap...')
                time.sleep(60*15)
                #sleep_until(e.retry_after) # sleep until the given date
            except TwythonError as e:
                # I *think* this is caused by protected profiles. 
                # I can read some user profile info, but not the timeline
                if e.error_code == 8:
                    # wait a minute
                    time.sleep(60*1)
                    # reconnect and try again
                    os.system('networksetup -setairportnetwork airport %s %s' % (opts.router_id, opts.router_password))
                    continue
                elif e.error_code in [401,404]:
                    #print(e)
                    #print("Skipping...")
                    break
                else:
                    print(e)
                    print(account)
                    print("Skipping...")
                    broken = True
                    break
        if not broken:
            with open(path, "w+") as tl:
                tl.write(json_string)
