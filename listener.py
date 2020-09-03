#!/usr/bin/env python

#USAGE: python listener.py -q portland -m d

import tweepy
import config
import redis
import json
import os
from json import dumps
from util import encode, connect_to_api, json_serial

redis = redis.StrictRedis(host='localhost', port=6379, db=0)

accountId = ['1602852614']

count = 0

class StreamListener(tweepy.StreamListener):
    
    def on_status(self, data):

        tweet = {}
        tweet['id'] = data.id_str
        tweet['created_at'] = dumps(data.created_at, default=json_serial)
        tweet['text'] = data.text
        tweet['username'] = data.user.screen_name
        tweet['url'] = data.user.url
        tweet['location'] = data.user.location
        json_tweet = json.dumps(tweet)
        
        redis.lpush('police', json_tweet)

        os.system('python upload.py -q police')

if __name__ == '__main__':
    api = connect_to_api(config)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(follow=accountId, languages=['en'], stall_warnings=True)





