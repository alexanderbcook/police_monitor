#!/usr/bin/env python
import tweepy
import config
import redis
import json
import os
from json import dumps
from util import connect_to_api, json_serial
from queue import Queue
from threading import Thread

redis = redis.StrictRedis(host='localhost', port=6379, db=0)

accountId = ['1602852614', '1606472113']

class StreamListener(tweepy.StreamListener):
    
    def __init__(self, queue = Queue()):
        number_worker_threads = 4
        self.queue = queue
        for i in range(number_worker_threads):
            thread = Thread(target=self.upload_tweet)
            thread.daemon = True
            thread.start()

    def on_data(self, data):

        jsonData = json.loads(data)

        tweet = {}
        tweet['id'] = jsonData['id_str']
        tweet['text'] = jsonData['text']
        tweet['username'] = jsonData['user']['screen_name']
        tweet['url'] = jsonData['user']['url']
        tweet['location'] = jsonData['user']['location']
        json_tweet = json.dumps(tweet)
        
        self.queue.put(redis.lpush('police', json_tweet))

    def upload_tweet(self):
        while True:
            self.queue.get()
            os.system('python upload.py -q police')
            self.queue.task_done()

if __name__ == '__main__':
    api = connect_to_api(config)
    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(follow=accountId, languages=['en'], stall_warnings=True)





