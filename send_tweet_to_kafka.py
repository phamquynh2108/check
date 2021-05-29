from loguru import logger
import sys
from tweepy import OAuthHandler, Stream# to authenticate Twitter API
from tweepy.streaming import StreamListener
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
                tweet = status.extended_tweet["full_text"]
            else:
                tweet = status.text

            # if "retweeted_status" attribute exists, flag this tweet as a retweet
            # if hasattr(status,"retweeted_status"):  #returns True if the specified object has the specified attribute
            # is_retweet = hasattr(status, "retweeted_status")
            # retweet = ""
            # if is_retweet:
            #     # check if quoted tweet's text has been truncated before recording it
            #     if hasattr(status, "extended_tweet"):  # Check if Retweet
            #         retweet = status.retweeted_status.extended_tweet["full_text"]
            #     else:
            #         retweet = status.retweeted_status.text
            #
            # is_quote = hasattr(status, "quoted_status")
            # quoted_tweet = ""
            # if is_quote:
            #     if hasattr(status.quoted_status, "extended_tweet"):
            #         quoted_tweet = status.quoted_status.extended_tweet["full_text"]
            #     else:
            #         quoted_tweet = status.quoted_status.text

            dict_ = {}
            dict_['date'] = str(status.created_at)
            dict_['tweet'] = tweet
           # dict_['retweet'] = retweet
            #dict_['quoted_tweet'] = quoted_tweet
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
        try:
            sendData()
            time.sleep(interval)
        except Exception as e:
            time.sleep(180)
            continue

if __name__ == "__main__":
    periodic_work(1)
    # in case you want to pass multiple criteria
