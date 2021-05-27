from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window, SparkSession
from pymongo import MongoClient
from nltk.corpus import stopwords
from itertools import chain
from test import *

stop_words = stopwords.words("english")

emoticons_pos = [":)", ":-)", ":p", ":-p", ":P", ":-P", ":D", ":-D", ":]", ":-]", ";)", ";-)", ";p", ";-p",
                 ";P", ";-P", ";D", ";-D", ";]", ";-]", "=)", "=-)", "<3"]
emoticons_neg = [":o", ":-o", ":O", ":-O", ":(", ":-(", ":c", ":-c", ":C", ":-C", ":[", ":-[", ":/", ":-/",
                 ":\\", ":-\\", ":n", ":-n", ":u", ":-u", "=(", "=-(", ":$", ":-$"]
# define positive emojis
emoji_pos = [u'\U0001f600', u'\U0001f601', u'\U0001f602', u'\U0001f923', u'\U0001f603', u'\U0001f604',
             u'\U0001f605', u'\U0001f606',
             u'\U0001f609', u'\U0001f60a', u'\U0001f60b', u'\U0001f60e', u'\U0001f60d', u'\U0001f618',
             u'\U0001f617', u'\U0001f619',
             u'\U0001f61a', u'\\U000263a', u'\U0001f642', u'\U0001f917']

# define negative emojis
emoji_neg = [u'\\U0002639', u'\U0001f641', u'\U0001f616', u'\U0001f61e', u'\U0001f61f', u'\U0001f624',
             u'\U0001f622', u'\U0001f62d',
             u'\U0001f626', u'\U0001f627', u'\U0001f628', u'\U0001f629', u'\U0001f62c', u'\U0001f630',
             u'\U0001f631', u'\U0001f633',
             u'\U0001f635', u'\U0001f621', u'\U0001f620', u'\U0001f612']
sample = stop_words + emoticons_pos + emoticons_neg + emoji_pos + emoji_neg

remove_characters = {' ': i for i in sample}

def save_tweet():
    appName = "TwitterSentimentAnalysis"
    master = "local[*]"
    spark = SparkSession.builder \
                     .appName(appName) \
                     .master(master) \
                     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
                     .config(" spark.sql.streaming.forceDeleteTempCheckpointLocation ", "true") \
                     .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    kafka_df = spark.readStream.format("kafka")\
                    .option("kafka.bootstrap.servers", "localhost:9092")\
                    .option("enable.auto.commit", "true")\
                    .option("auto.commit.interval.ms", "5000")\
                    .option("subscribe", "crawl_tweet").load()

    lines = kafka_df.selectExpr("CAST(value AS STRING) as json") \
                    .select(json_tuple(col("json"), "date", "text", "retweeted_text", "quoted_text", "favourites_count")) \
                    .toDF("date", "text", "retweeted_text", "quoted_text", "favourites_count") \
                    .withColumn("date", from_unixtime(unix_timestamp('date', 'yyyy-MM-dd HH:mm:ss')))\
                    .withColumn('classify', when((lower(col('text')).contains('iphone') | \
                                                  lower(col('text')).contains('macbook pro') | \
                                                  lower(col('text')).contains('airpods') | \
                                                  lower(col('text')).contains('macbook') | \
                                                  lower(col('text')).contains('apple') | \
                                                  lower(col('text')).contains('ipod')), 'apple').otherwise('samsung'))

    lines = decontract_text(lines, cols='text')
    lines = replace_repeating_characters(lines, cols='text')
    lines = replace_specific_characters(lines, cols='text')
    lines = replace_punctuation(lines, cols='text')
    # mapping_expr = create_map(lit(x) for x in chain(*remove_characters.items()))
    # lines = lines.withColumn('text2', mapping_expr[lines['text']])

    map_function = udf(lambda row: remove_characters.get(row, row))
    lines = lines.withColumn("text2", map_function(col('text')))


    # def write_df_mongo(target_df):
    #     url = "mongodb://localhost:27017"
    #     cluster = MongoClient(url)
    #     db = cluster["twitter"]
    #     collection = db.raw_tweet
    #
    #     post = {
    #             "date": target_df.date,
    #             "text": target_df.text,
    #             "retweeted_text": target_df.retweeted_text,
    #             "quoted_text": target_df.quoted_text,
    #             "favourites_count": target_df.favourites_count,
    #             }
    #
    #     collection.insert_one(post)
    #
    # lines.writeStream\
    #     .outputMode("append")\
    #     .foreach(write_df_mongo)\
    #     .trigger(processingTime='60 seconds')\
    #     .start().awaitTermination()


    # write to console
    query = lines.writeStream \
            .trigger(processingTime='7 seconds') \
            .format("console") \
            .option("truncate", False) \
            .start()

    query.awaitTermination()


if __name__ == "__main__":
    save_tweet()