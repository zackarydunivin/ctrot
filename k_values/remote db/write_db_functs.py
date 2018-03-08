import gzip
from datetime import datetime, timedelta
import emoji
import time
import json

#Functions for write_db.py

def gzip_timeline_generator(filename):
    with gzip.open(filename, 'rb') as f:
        for line in f:
            yield line

def parse_urls(tweet):
    urls = []
    if not tweet['entities']['urls']:
        return None
    else:
        for u in tweet['entities']['urls']:
            # unwound gives us all the interesting information
            # including actual url rather than the bit.ly url
            try:
                print(u['unwound'])
                urls += u['unwound']
            except KeyError:
                pass
    if not urls:
        return None
    return json.dumps(urls)

def parse_mentions(tweet):
    # all user ids mentioned in this tweet
    mentions = []
    if not tweet['entities']['user_mentions']:
        return None
    else:
        for m in tweet['entities']['user_mentions']:
            mentions.append(m['id'])
    return json.dumps(mentions)

def bulkInsert(c,conn,doclist,table,fields):
    query = 'INSERT IGNORE INTO ' + table + ' VALUES (' + ','.join(['"%s"']*len(fields)) + ')'
    query += ' ON DUPLICATE KEY UPDATE tweet_id=tweet_id'
    try:
        c.executemany(query,doclist)
    except Exception as inst:
        print inst
        print "Beginning to insert manually..."
        for doc in doclist:
            try:
                c.execute(query,doc)
            except Exception as innerinst:
                print innerinst
                print "ERROR DOC"
                print doc
                continue
    conn.commit()
    return [],[]
    
def createIndices():
    indices = [('user_id_index','user_id'),
               ('date_index','created_at')]
    for i in indices:
        try:
            c.execute("CREATE INDEX %s ON tweets (%s);" % i)
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

def createUniqueIndices(c):
    try:
        c.execute("CREATE UNIQUE INDEX id_index ON tweets (tweet_id);")
    except Exception as inst:
        print(inst)

    try:
        c.execute("CREATE UNIQUE INDEX id_index ON retweets (tweet_id);")
    except Exception as inst:
        print(inst)

    try:
        c.execute("CREATE UNIQUE INDEX id_index ON mentions (tweet_id);")
    except Exception as inst:
        print(inst)
        
#Functions for tweets
def parseTweet(tweet):
    tweet_fields = ['tweet_id','user_id','text','source','created_at','created_at_string','in_reply_to_status_id','in_reply_to_user_id','longitude',
                    'latitude','place','quoted_status_id','quoted_status','retweeted_status','quote_count','reply_count','retweet_count',
                    'favorite_count','user_mentions','urls','possibly_sensitive','filter_level','lang']
    date = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')

    # change the emoji from unicode to text
    list_text = list(tweet['text'])
    for i in range(len(tweet['text'])):
        if list_text[i] in emoji.UNICODE_EMOJI:
            try:
                list_text[i] = emoji.UNICODE_EMOJI_ALIAS[tweet['text'][i]]
            except:
                list_text[i] = '\U0001f95a'

    tweet['text'] = ''.join(list_text)

    selected_values = []
    try:
        for field in tweet_fields:
            # if field == 'created_at':
            #     selected_values.append(int(date.timestamp()))
            if field == 'tweet_id':
                selected_values.append(tweet['id'])
            elif field == 'created_at_string':
                d = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
                selected_values.append(d)
            elif field == 'urls':
                selected_values.append(parse_urls(tweet))
            elif field == 'user_mentions':
                selected_values.append(parse_mentions(tweet))
            elif field == 'longitude':                    
                if tweet['coordinates']:
                    selected_values.append(tweet['coordinates']['coordinates'][0])
                else:
                    selected_values.append(None)
            elif field == 'latitude':
                if tweet['coordinates']:
                    selected_values.append(tweet['coordinates']['coordinates'][1])
                else:
                    selected_values.append(None)
            elif field == 'user_id':
                selected_values.append(tweet['user']['id'])
            elif field in ['quoted_status_id','quote_count','reply_count','possibly_sensitive','filter_level']:
                try:
                    selected_values.append(tweet[field])
                except KeyError:
                    selected_values.append(None)
            elif field in ['quoted_status','retweeted_status']:
                try:
                    selected_values.append(tweet[json.dumps(field)])
                except KeyError:
                    selected_values.append(None)
            else:
                selected_values.append(tweet[field])
    except Exception as inst:
        print(inst)
        print(tweet)
        raise ValueError("quit early")
    return selected_values

#Functions for retweets and mentions
def generalDicter(tweet):
    general_dict = {}            
    general_dict['tweet_id'] = tweet['id']
    general_dict['outgoing_user_id'] = (tweet['user']['id'])
    #general_dict['created_at'] = (int(date.timestamp()))
    general_dict['created_at_string'] = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
    return general_dict

def parseRetweets(tweet):
    retweet_fields = ['tweet_id','outgoing_user_id','incoming_user_id','created_at_string','retweeted_status_id','is_quote']
    retweet_dict = generalDicter(tweet)
    # retweet
    try:
        retweet = tweet['retweeted_status']['user']['id']
        retweet_dict['incoming_user_id'] = retweet
        retweet_dict['retweeted_status_id'] = tweet['retweeted_status']['id']
        retweet_dict['is_quote'] = 0
    except KeyError:
        pass
    # quote
    try: 
        retweet = tweet['quoted_status']['user']['id']
        retweet_dict['incoming_user_id'] = retweet
        retweet_dict['retweeted_status_id'] = tweet['quoted_status']['id']
        retweet_dict['is_quote'] = 1
    except KeyError:
        pass
    return [retweet_dict[k] if k in retweet_dict else 'None' for k in retweet_fields]
    
def parseMentions(tweet):
    mention_fields = ['tweet_id','outgoing_user_id','incoming_user_id','created_at_string','is_reply','in_reply_to_status_id']
    mention_dict = generalDicter(tweet)
    mention_dict['incoming_user_id'] = parse_mentions(tweet)
    #Handling replies
    if tweet['in_reply_to_user_id']:
        mention_dict['is_reply'] = 1
        mention_dict['in_reply_to_status_id'] = tweet['in_reply_to_user_id']
    return [mention_dict[k] if k in mention_dict else 'None' for k in mention_fields]
