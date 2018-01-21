from glob import glob
import os
import time
from tqdm import tqdm


if __name__ == "__main__":
    import sys
    import argparse

    # echo command line immediately...
    sys.stderr.write("%s\n" % " ".join(sys.argv))
    sys.stderr.flush()

    # parse command line...
    parser = argparse.ArgumentParser(description='Command line for manual_twitter_scrape.py.')
    # main function arguments...
    parser.add_argument('-users_glob', type=str, required=True)

    opts = parser.parse_args()


    for fname in glob(opts.users_glob):
        with open(fname,'r') as user_ids_f:
            user_ids = list(set([x.replace('\n','') for x in user_ids_f.readlines()]))
        
        folder = 'db/timelines/' + os.path.splitext(fname)[0][5:]
        
        # check to see if we still have this one decompressed
        # I deleted some to make space on the drive
        path = "%s" % (folder)
        if not os.path.exists(path):
            continue

        for account in user_ids:
            path = "%s/%s.json" % (folder,account)
            if os.path.exists(path):
                continue
            else:
                print(account)
