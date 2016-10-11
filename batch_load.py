# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 11:09:41 2015

@author: yuxuanzhang
"""
import json, time


################################
##### load tweets with batching - less burdon for RAM
################################
def batch_load_Tweets(tweets):

    # Collect multiple tweets so that we can use "executemany".  We do
    # not want to collect all of the tweets at once because there may
    # not be enough memory for that. So we a batch of tweets at a time
    batchSize = 2
    batchedInserts = []

    # as long as there is at least one tweet remaining in the file
    while len(tweets) > 0:
        tweet = tweets.pop(0) # take the first tweet from the file and remove it
    
        tweets_parsed = json.loads(tweet)

        newTweet = [] # hold values to-be-inserted from the tweet
        tweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors']

        for key in tweetKeys:
            # Treat '', [] and 'null' as NULL
            if tweets_parsed[key] in ['',[],'null']:
                newTweet.append(None)
            else:
                newTweet.append(tweets_parsed[key])

        # Add the new tweet values to the collected batch list
        batchedInserts.append(newTweet)
        
        # If we have reached # of batchSize, use executemany to insert what we collected
        # so far, and reset the batchedInserts list back to empty
        if len(batchedInserts) >= batchSize or len(tweets) == 0:
            #print batchedInserts            
            
            #c.executemany('INSERT INTO Tweets VALUES(?,?,?,?,?,?,?,?,?)', batchedInserts)
            
            # Reset the batching process
            batchedInserts = []

start = time.time()
fd = open('Assignment4.txt', 'r')
tweets = fd.readline().split('EndOfTweet')[:5] 

batch_load_Tweets(tweets)

end   = time.time()

print ("loadTweets took ", (end-start), ' seconds.')