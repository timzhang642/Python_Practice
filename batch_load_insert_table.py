# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 15:05:00 2015

@author: yuxuanzhang
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 23:39:51 2015

@author: yuxuanzhang
"""

import sys
import sqlite3 
import json
import time 

# return a list that contains individual user dic
def get_users_list():
    
    users_list = [] # this list contains user dic, which are sub-dic under tweet dic
    
    for tweet in batchedTweets:
        users_list.append(tweet['user'])
    
    return users_list

# return a list that contains individual geo dic
def get_geo_list():
    
    geo_list = [] # this list contains geo dic, which are sub-dic under tweet dic
    
    for tweet in batchedTweets:
        if tweet['geo'] != None:     
            geo_list.append(tweet['geo'])
        
    return geo_list
    
# this function run queries that create and populate geo table
def populate_geo_table(connection):
    
    # read online txt data
    geo_list = get_geo_list()
    
    insert_geo_list = [] # put data into a nested list, each sublist is a row of a geo data for insertion
    
    for geo in geo_list:
        row_list = [] #this list holds user elements of each row, will be used for row inseration
        if geo['coordinates'][0] not in coordinate_dic.keys():
            coordinate_dic[geo['coordinates'][0]] = geo['coordinates'][1]
            
            row_list.append(geo['type'])
            row_list.append(geo['coordinates'][0])
            row_list.append(geo['coordinates'][1])
            insert_geo_list.append(row_list)
    
    insert_record = "INSERT INTO geo VALUES(?, ?, ?)"

    connection.executemany(insert_record, insert_geo_list)

    connection.commit()

# this function run queries that create and populate users table
def populate_users_table(connection):
    
    # read online txt data
    users_list = get_users_list()

    
    insert_users_list = [] # put data into a nested list, each sublist is a row of a user data
    
    for user in users_list:
        row_list = [] #this list holds user elements of each row, will be used for row inseration

        if user['id'] not in user_id_list:
            user_id_list.append(user['id']) 
            
            row_list.append(user['id']) 
            row_list.append(user['description'])
            row_list.append(user['friends_count'])
            row_list.append(user['name'])
            row_list.append(user['screen_name'])
            insert_users_list.append(row_list)
    
    insert_record = "INSERT INTO users VALUES(?, ?, ?, ?, ?)"

    connection.executemany(insert_record, insert_users_list)

    connection.commit()

def populate_tweets_table_with_foreign_key(connection):
    
    insert_tweet_list = []
    
    
    for tweet in batchedTweets:
        row_list = [] #this list holds user elements of each row, will be used for row inseration
        
        if tweet['id_str'] not in id_list: #make sure id_str is unique
            id_list.append(tweet['id_str'])
            
            row_list.append(tweet['id_str'])
            row_list.append(tweet['created_at']) 
            row_list.append(tweet['text'])
            row_list.append(tweet['source'])
            row_list.append(tweet['in_reply_to_user_id'])
            row_list.append(tweet['in_reply_to_screen_name']) 
            row_list.append(tweet['in_reply_to_status_id'])
            try:
                row_list.append(tweet['retweeted_status']['retweet_count'])
            except:
                row_list.append(tweet['retweet_count'])
            row_list.append(tweet['contributors'])
            row_list.append(tweet['user']['id'])
            try:
                row_list.append(tweet['geo']['coordinates'][0])
                row_list.append(tweet['geo']['coordinates'][1])
            except:
                row_list.append(None)
                row_list.append(None)
            insert_tweet_list.append(row_list)    
    
    
    insert_record = "INSERT INTO tweets VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    connection.executemany(insert_record, insert_tweet_list)
    connection.commit()

def create_three_tables(connection):
    # create geo table, #composite primary key
    query1 = """
    CREATE TABLE geo
    (
    type VARCHAR(20),
    longitude NUMBER(8,6),
    latitude NUMBER(8,6),
    CONSTRAINT table_pk PRIMARY KEY(longitude,latitude)
    );"""

    try:    
        connection.execute(query1)
    except:
        print sys.exc_info()[0] #print out error 
        pass # do not create table if it already exists
    
    #------------------------------------------------------
    # create users table
    query2 = """
    CREATE TABLE users
    (
    id NUMBER(18) NOT NULL,
    description VARCHAR(5000),
    friends_count NUMBER(10),
    name VARCHAR(100),
    screen_name VARCHAR(100),
    CONSTRAINT table_pk PRIMARY KEY(id)
    );"""

    try:    
        connection.execute(query2)
    except:
        print sys.exc_info()[0] #print out error 
        pass # do not create table if it already exists
    
    #------------------------------------------------------
    # create a tweets table
    query3 = """
    CREATE TABLE tweets
    (
    id_str NUMBER(18) NOT NULL,
    created_at datetime NOT NULL,
    text VARCHAR(255),
    source VARCHAR(255) NOT NULL,
    in_reply_to_user_id NUMBER(18),
    in_reply_to_screen_name VARCHAR(255),
    in_reply_to_status_id NUMBER(18),
    retweet_count NUMBER(6) NOT NULL,
    contributors VARCHAR(225),
    user_id NUMBER(18),
    longitude NUMBER(8,6),
    latitude NUMBER(8,6),
    CONSTRAINT table_pk PRIMARY KEY(id_str)
    FOREIGN KEY (user_id) REFERENCES users(id)
    FOREIGN KEY (longitude,latitude) REFERENCES geo(longitude,latitude)
    );"""
    
    try:    
        connection.execute(query3)
    except:
        print sys.exc_info()[0] #print out error       
        pass # do not create table if it already exists
    
def populate_three_tables(connection):
    
    populate_geo_table(connection) #question 1.a
    populate_users_table(connection)
    populate_tweets_table_with_foreign_key(connection)


#main
start = time.time()

connection = sqlite3.connect('final_question1_e.db') #create/connect to a local database

local_file = open('1_b.txt','r') # open tweet file in txt format
tweets = local_file.readlines()

create_three_tables(connection)

# initiate the following to make sure primary keys are unique
coordinate_dic = {} # includes unique longitude:(latitude) pairs
user_id_list = [] # includes unique user id
id_list = [] # includes unique id_str


# batch load tweets and populate tables
batchSize = 500
batchedTweets = []

# as long as there is at least one tweet remaining in the file
while len(tweets) > 0:
    tweet = tweets.pop(0) # take the first tweet from the file and remove it

    try:
        #tweets_parsed = json.loads(tweet)
        # Add the new tweet values to the collected batch list
        batchedTweets.append(json.loads(tweet))
    except:
        print 'A tweet is damaged.'         
        pass
    
    # If we have reached # of batchSize, use executemany to insert what we collected
    # so far, and reset the batchedTweets list back to empty
    if len(batchedTweets) >= batchSize or len(tweets) == 0:
        #print batchedTweets,'\n'            
        
        populate_three_tables(connection)
        
        # Reset the batching process
        batchedTweets = []


# put query result into a dataframe
import pandas.io.sql as sql
print sql.read_sql('select * from geo LIMIT 5', connection),'\n'
print sql.read_sql('select * from users LIMIT 5', connection),'\n'
print sql.read_sql('select * from tweets LIMIT 5', connection)

end = time.time()
print "Question 1.e took", (end-start), 'seconds.'