#sqlite_utilities.py

import sqlite3




FOLLOWERS_TABLE_NAME = 'followers'
TWEETS_TABLE_NAME = 'tweets'
FOLLOWERS_DB_PATH = 'db/us_wn_followers.db'
TWEETS_DB_PATH = 'db/us_wn_tweets.db'

def sqlite_connect(dbpath):
    conn = sqlite3.connect(dbpath)
    return conn

def sqlite_get_cursor(conn):
    cursor = conn.cursor() 
    return cursor

def prepare_followers_table(cursor,table_name):
    cursor.execute('''CREATE TABLE IF NOT EXISTS %s
                 (user_id text, followers,degree CLOB,user_json)''' % table_name)
    cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS user_id_index
                    ON %s (user_id)''' % table_name)

def prepare_tweets_table(cursor,table_name):
    cursor.execute('''CREATE TABLE IF NOT EXISTS %s
                 (tweet_id text, tweet,user,created_at CLOB)''' % table_name)
    cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS user_id_index
                    ON %s (tweet_id)''' % table_name)

def sqlite_followers_insert(cursor,table_name,user_id,followers,user_json,degree=-1):
    # default -1 indicates the degree in the snowball is unknown
    cursor.execute("INSERT OR REPLACE INTO %s VALUES (?,?,?,?)" % table_name, (user_id,followers,degree,user_json))

def sqlite_tweet_insert(cursor,table_name,tweet_id,tweet,user,created_at):
    cursor.execute("INSERT OR REPLACE INTO %s VALUES (?,?,?,?)" % table_name, (tweet_id,tweet,user,created_at))

def sqlite_check_for_user_id(cursor,table_name,user_id):
	# return True if id in index, else false
	return cursor.execute('SELECT * FROM %s WHERE user_id=?' % table_name, (user_id,)).fetchone()

def sqlite_check_for_tweet_id(cursor,table_name,tweet_id):
    # return True if id in index, else false
    return cursor.execute('SELECT * FROM %s WHERE tweet_id=?' % table_name, (tweet_id,)).fetchone()

def parse_followers(followers_string):
    # input: comma separated ids e.g., '124123, 18234, 91233'
    # output: list of integer ids
    if not followers_string:
        return []
    else:
        followers_list = followers_string.split(', ')
        return [int(_id) for _id in followers_list]
