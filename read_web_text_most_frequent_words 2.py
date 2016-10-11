# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 11:44:38 2015

@author: yuxuanzhang
"""

import urllib2
import json
import time

def read_web_text():
    
    web_file = urllib2.urlopen('http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/Assignment5.txt')
    tweets = web_file.readlines()
    
    tweets_list = [] # this list contains tweet dic
    error_tweets_list = [] # this list contains damaged tweet dics that cannot be parsed by json
    
    for tweet in tweets:
        try:
            tweets_list.append(json.loads(tweet))
        except:
            error_tweets_list.append(tweet) # these are damaged tweets
    
    return tweets_list
    
    
def top_k_frequent_words(tweets_list,k):
    # count words from tweet text
    tweet_text_list = []
    for tweet in tweets_list:
        tweet_text_list.append(tweet['text'])
    
    word_dic = {} # {word: count, word2: count...}
    
    for text in tweet_text_list:
        words_list = text.split(' ')
        for word in words_list:
            if not word_dic.has_key(word):
                word_dic[word]=1
            else:
                word_dic[word] += 1
    
    key_value_pairs=word_dic.items() # [(key,value),(key,value)]
    key_value_pairs.sort(reverse=True,key=lambda x: x[1]) # sort based on value, rank in descending order
        
    
    return key_value_pairs[:k]
    
    
# main
start = time.time()
    
# read online txt data
tweets_list = read_web_text()

print 'top 3 most frequent words:',top_k_frequent_words(tweets_list, 3)


end = time.time() 
print "time spent:",end - start