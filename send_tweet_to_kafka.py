from loguru import logger
import sys
from tweepy import OAuthHandler, Stream# to authenticate Twitter API
from tweepy.streaming import StreamListener
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
from variable import *

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC ="crawl_tweet"

class TweetsListener(StreamListener):
    def on_status(self, status):
        try:
            #check if text has been truncated
            if hasattr(status,"extended_tweet"):
                text = status.extended_tweet["full_text"]
            else:
                text = status.text

            # if "retweeted_status" attribute exists, flag this tweet as a retweet
            # if hasattr(status,"retweeted_status"):  #returns True if the specified object has the specified attribute
            is_retweet = hasattr(status, "retweeted_status")
            retweeted_text = ""
            if is_retweet:
                # check if quoted tweet's text has been truncated before recording it
                if hasattr(status, "extended_tweet"):  # Check if Retweet
                    retweeted_text = status.retweeted_status.extended_tweet["full_text"]
                else:
                    retweeted_text = status.retweeted_status.text

            is_quote = hasattr(status, "quoted_status")
            quoted_text = ""
            if is_quote:
                if hasattr(status.quoted_status, "extended_tweet"):
                    quoted_text = status.quoted_status.extended_tweet["full_text"]
                else:
                    quoted_text = status.quoted_status.text

            dict_ = {}
            dict_['date'] = str(status.created_at)
            dict_['text'] = text
            dict_['retweeted_text'] = retweeted_text
            dict_['quoted_text'] = quoted_text
            dict_['favourites_count'] = status.user.favourites_count

            print(dict_)
            global producer
            producer.send(KAFKA_TOPIC, json.dumps(dict_).encode('ascii'))
        except Exception as e:
            logger.error('[{}] : {}'.format(sys._getframe().f_code.co_name, e))
            exit(1)

    def on_error(self, status_code):
        if status_code == 420:
            return False

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
    logger.error('Error Connecting to Kafka {} : {}'.format(sys._getframe().f_code.co_name, e))
    sys.exit(0)

def sendData():
    logger.info('Called function: {}'.format(sys._getframe().f_code.co_name))

    auth = OAuthHandler(os.environ['CONSUMER_KEY'], os.environ['CONSUMER_KEY_SECRET'])
    auth.set_access_token(os.environ['ACCESS_TOKEN'], os.environ['ACCESS_TOKEN_SECRET'])

    twitter_stream = Stream(auth, TweetsListener())
    twitter_stream.filter(track=list, languages=['en'], encoding='utf8')

def periodic_work(interval):
    while True:
        sendData()
        #interval should be an integer, the number of senconds to wait
        time.sleep(interval)

if __name__ == "__main__":
    periodic_work(1)
    # in case you want to pass multiple criteria
