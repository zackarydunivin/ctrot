import os
import gzip as gz
import json
from datetime import datetime
import pickle

dc2_folder = "/N/dc2/scratch/vmwong/"
data_folder = "us_wn_timelines_gz/"
rt_files_folder = "us_wn_timelines_rt/"
usermeta_folder = "usermeta/"
files = [x for x in os.listdir(dc2_folder + data_folder) if 'merged_us_wn' in x]

rt_files_done = set([])

for fn in files:
    userdata = {}
    #Metadata
    with gz.open(dc2_folder + data_folder + fn,'rt') as data:
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
        
            if 'retweeted_status' in l:
                datestr = datetime.strptime( l['created_at'], "%a %b %d %H:%M:%S +0000 %Y").strftime("%Y-%m-%d")
                if datestr+".csv" not in rt_files_done:
                    rt_files_done.add(datestr+".csv")
                    with open(dc2_folder+rt_files_folder + datestr + ".csv", 'w') as f:
                        f.write("Sender, Receiver\n")
                with open(dc2_folder+rt_files_folder + datestr +".csv", 'a') as f:
                    f.write( "%i, %i\n"%(user, l['retweeted_status']['user']['id']) )
                    
        with open(dc2_folder + usermeta_folder + 'user_metadata+'+datestr+'.pickle','wb') as f:
            pickle.dump(userdata, f)