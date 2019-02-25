import os
import gzip as gz
import json
from datetime import datetime

dc2_folder = "/N/dc2/scratch/vmwong/"
data_folder = "us_wn_timelines_gz/"
rt_files_folder = "us_wn_timelines_rt/"
files = [x for x in os.listdir(dc2_folder + data_folder) if 'merged_us_wn' in x]

userdata = {}
rt_files_done = set([])

for fn in files:
    #Metadata
    data = gz.open(dc2_folder + data_folder + 'merged_us_wn_1degree_followers_unique_aa.gz','rt')
    for line in data:
        l = json.loads(line.replace(",\n",""))
        user = l['user']['id']
        if user not in userdata:
            userdata[user] = {'first_tweet': l['created_at'], 'tweets': []}
        #Created_at earliest date
        if userdata[user]['first_tweet'] < l['created_at']:
            userdata[user]['first_tweet'] = l['created_at']
        #For user timeseries, if tweet, append 't'. If retweet, append 'r'
        if 'retweeted_status' in l:
            userdata[user]['tweets'].append( (l['created_at'],'r') )
        else:
            userdata[user]['tweets'].append( (l['created_at'],'t') )
    
        #Creating retweet files
        if 'retweeted_status' in l:
            datestr = datetime.strptime( l['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d")
            if datestr+".csv" not in rt_files_done:
                rt_files_done.add(datestr+".csv")
                with open(dc2_folder + rt_files_folder + datestr + ".csv", 'w') as f:
                    f.write("Sender, Receiver\n")
            with open(dc2_folder + rt_files_folder + datestr +".csv", 'a') as f:
                f.write( "%i, %i\n"%(user, l['retweeted_status']['user']['id']) )

with open(dc2_folder + 'user_metadata.pickle','wb') as f:
    pickle.dump(userdata,f)
