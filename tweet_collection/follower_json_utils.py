# follower_json_utilities.py
import json
import numpy as np
from collections import Counter
# -*- coding: UTF-8 -*-


def load_json_file(fname):
    accounts = []
    with open(fname) as f:
        for line in f:
            accounts.append(json.loads(line))
    return accounts

def list_of_json_objects_to_dict(accounts):
    accnt_dict = {}
    for accnt in accounts:
        accnt_dict[int(accnt.keys()[0])] = accnt.values()[0]
    return accnt_dict

def filter_followers_to_include_only_seeds(accounts):
    return filter_followers_to_include_only_these(accounts,accounts(keys))

def filter_followers_to_include_only_these(accounts,interesting_accounts):
    for accnt in accounts:
        filtered = set(accounts[accnt]).intersection(set(interesting_accounts))
        accounts[accnt] = list(filtered)
    return accounts

def get_unique_followers(accounts):
    """input: dictionary where keys are user ids and 
    values are lists of that users followers ids
    output: a set of all followers (without duplicates)
    """
    unique_followers = set()
    for accnt in accounts:
        accnt_followers = set(accounts[accnt])
        unique_followers = unique_followers.union(accnt_followers)

    return unique_followers

def get_unique_users(accounts):
    unique_followers = get_unique_followers(accounts)
    return unique_followers.union(set(accounts.keys()))

def get_unique_followers_without_seeds(accounts):
    unique_followers = get_unique_followers(accounts)
    return unique_followers.difference(set(accounts.keys()))

def count_all_followers(accounts):
    count = 0
    for followers in accounts.values():
        count += len(followers)
    return count

def out_degree_histogram(accounts):
    all_followers = []
    for followers in accounts.values():
        all_followers += followers


    counter = Counter(all_followers)
    counts = dict(counter).values()

    return np.histogram(counts,bins=range(1,len(accounts)+1))

if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for follower_json_utilities.py.')
    # main function arguments...
    parser.add_argument('-json_fname', type=str, required=True)

    opts = parser.parse_args()

    accounts = load_json_file(opts.json_fname)
    accounts = list_of_json_objects_to_dict(accounts)

    print("Total number of followers: %d" % count_all_followers(accounts))
    print("Total number of unique followers: %d" % len(get_unique_followers(accounts)))
    print("\nOut degree histogram:")
    print(out_degree_histogram(accounts))

    with open('us_wn_1stdegree_followers_minus_unique.txt','w') as f:
        for follower in get_unique_followers_without_seeds(accounts):
            f.write('%r\n' %follower)