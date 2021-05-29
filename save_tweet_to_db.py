from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pymongo import MongoClient
from nltk.corpus import stopwords
from test import *
from nltk.stem.wordnet import WordNetLemmatizer
from textblob import TextBlob
from transformers import BertTokenizer,BertForSequenceClassification

output_dir = './outputs'
tokenizer = BertTokenizer.from_pretrained(output_dir)
model_loaded = BertForSequenceClassification.from_pretrained(output_dir)

lemmatizer = WordNetLemmatizer()
stop_words = stopwords.words("english")

emoticons_pos = [":)", ":-)", ":p", ":-p", ":P", ":-P", ":D", ":-D", ":]", ":-]", ";)", ";-)", ";p", ";-p",
                 ";P", ";-P", ";D", ";-D", ";]", ";-]", "=)", "=-)", "<3"]
emoticons_neg = [":o", ":-o", ":O", ":-O", ":(", ":-(", ":c", ":-c", ":C", ":-C", ":[", ":-[", ":/", ":-/",
                 ":\\", ":-\\", ":n", ":-n", ":u", ":-u", "=(", "=-(", ":$", ":-$"]
# define positive emojis
emoji_pos = ['\U0001f600', '\U0001f601', '\U0001f602', '\U0001f923', '\U0001f603', '\U0001f604',
             '\U0001f605', '\U0001f606',
             '\U0001f609', '\U0001f60a', '\U0001f60b', '\U0001f60e', '\U0001f60d', '\U0001f618',
             '\U0001f617', '\U0001f619',
             '\U0001f61a', '\\U000263a', '\U0001f642', '\U0001f917']

# define negative emojis
emoji_neg = ['\\U0002639', '\U0001f641', '\U0001f616', '\U0001f61e', '\U0001f61f', '\U0001f624',
             '\U0001f622', '\U0001f62d',
             '\U0001f626', '\U0001f627', '\U0001f628', '\U0001f629', '\U0001f62c', '\U0001f630',
             '\U0001f631', '\U0001f633',
             '\U0001f635', '\U0001f621', '\U0001f620', '\U0001f612']

sample = stop_words + emoticons_pos + emoticons_neg + emoji_pos + emoji_neg
remove_punc_stopword = {' ': i for i in sample}

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
                    .select(F.json_tuple(F.col("json"), "date", "tweet", "favourites_count")) \
                    .toDF("date", "tweet", "favourites_count") \
                    .withColumn('raw_tweet', F.col('tweet'))\
                    .withColumn("date", F.from_unixtime(F.unix_timestamp('date', 'yyyy-MM-dd HH:mm:ss')))\
                    .withColumn('classify', F.when((F.lower(F.col('tweet')).contains('iphone') | \
                                                    F.lower(F.col('tweet')).contains('macbook pro') | \
                                                    F.lower(F.col('tweet')).contains('airpods') | \
                                                    F.lower(F.col('tweet')).contains('macbook') | \
                                                    F.lower(F.col('tweet')).contains('apple') | \
                                                    F.lower(F.col('tweet')).contains('ipod')), 'apple').otherwise('samsung'))

    lines = decontract_text(lines, cols='tweet')
    lines = replace_repeating_characters(lines, cols='tweet')
    lines = replace_specific_characters(lines, cols='tweet')
    lines = replace_punctuation(lines, cols='tweet')

    remove_punc = F.udf(lambda row: remove_punc_stopword.get(row, row))
    lines = lines.withColumn("tweet", remove_punc(F.col('tweet'))) \
                 .withColumn('tweet', F.regexp_replace(F.col("tweet"), "\\s+", " ")) \
                 .withColumn('tweet', F.trim(F.col('tweet')))\
                 .withColumn('tweet', F.lower(F.col('tweet')))

    sentiment_tweet = F.udf(lambda row: Sentiment(tokenizer, model_loaded, row), ArrayType(StringType()))
    lines = lines.withColumn("result", sentiment_tweet(F.col('tweet')))\
                 .withColumn("predict", F.col('result').getItem(0))\
                 .withColumn("label", F.col('result').getItem(1)) \
                 .withColumn('nor_predict', (F.col('predict')*2-1) )

    get_Wordcloud = F.udf(lambda row: get_wordcloud(row), ArrayType(StringType()))
    lines = lines.withColumn("Wordcloud", get_Wordcloud(F.col('tweet'))) \
                 .select('date', 'raw_tweet', 'tweet', 'classify', 'predict', 'nor_predict', 'label', 'favourites_count', 'Wordcloud')

    def write_df_mongo(df):
        url = "mongodb://localhost:27017"
        cluster = MongoClient(url)
        db = cluster["twitter"]
        collection = db.raw_tweet

        post = {
                "date": df.date,
                "raw_tweet": df.raw_tweet,
                "tweet": df.tweet,
                "classify": df.classify,
                "predict": df.predict,
                "label": df.label,
                "favourites_count": df.favourites_count,
                }

        collection.insert_one(post)

    lines.writeStream\
        .outputMode("append")\
        .foreach(write_df_mongo)\
        .trigger(processingTime='20 seconds')\
        .start().awaitTermination()

if __name__ == "__main__":
    save_tweet()