#!/usr/bin/env python

#USAGE: python listener.py -q portland -m d

import tweepy
import logging
import config
import redis
import json
import os
from json import dumps
from util import encode, connect_to_api, json_serial

redis = redis.StrictRedis(host='localhost', port=6379, db=0)
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

accountId = ['1602852614']

count = 0

class StreamListener(tweepy.StreamListener):
    
    def on_status(self, data):
        global count
        count += 1

        if count % 100 == 0:
            logging.debug('%s tweets gathered.' % str(count))

        tweet = {}
        tweet['id'] = data.id_str
        tweet['created_at'] = dumps(data.created_at, default=json_serial)
        tweet['text'] = data.text
        tweet['username'] = data.user.screen_name
        tweet['url'] = data.user.url
        tweet['location'] = data.user.location
        json_tweet = json.dumps(tweet)
        
        redis.lpush('police', json_tweet)
        i = redis.llen('police')

        if i >= 1:
            os.system('python upload.py -q police')

if __name__ == '__main__':
    api = connect_to_api(config)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(follow=accountId, languages=['en'])





