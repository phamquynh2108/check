import string

import pyspark.sql
import pyspark.sql.streaming
from variable import *
import string
from variable import *
from pyspark.sql.functions import *


def decontract_text(df: pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    # specific
    df = df.withColumn(cols, regexp_replace(cols, "\n", " ")) \
        .withColumn(cols, regexp_replace(cols, '&amp;', ' and ')) \
        .withColumn(cols, regexp_replace(cols, r'@.*?( |$)', '')) \
        .withColumn(cols, regexp_replace(cols, r'http[s]{0,1}.*?( |$)', '')) \
        .withColumn(cols, regexp_replace(cols, r'#', '')) \
        .withColumn(cols, regexp_replace(cols, r"won't", "will not")) \
        .withColumn(cols, regexp_replace(cols, r"won't", "will not")) \
        .withColumn(cols, regexp_replace(cols, r"can\'t", "can not")) \
        .withColumn(cols, regexp_replace(cols, r"n\'t", " not")) \
        .withColumn(cols, regexp_replace(cols, r"n\u2019t", " not")) \
        .withColumn(cols, regexp_replace(cols, r"\'re", " are")) \
        .withColumn(cols, regexp_replace(cols, r"\'s", " is")) \
        .withColumn(cols, regexp_replace(cols, r"\u2019s", " is")) \
        .withColumn(cols, regexp_replace(cols, r"\'d", " would")) \
        .withColumn(cols, regexp_replace(cols, r"\'ll", " will")) \
        .withColumn(cols, regexp_replace(cols, r"\'t", " not")) \
        .withColumn(cols, regexp_replace(cols, r"\'ve", " have")) \
        .withColumn(cols, regexp_replace(cols, r"\'m", " am")) \
        .withColumn(cols, regexp_replace(cols, r"don\x89Ûªt", "do not")) \
        .withColumn(cols, regexp_replace(cols, r"I\x89Ûªm", "I am")) \
        .withColumn(cols, regexp_replace(cols, r"you\x89Ûªve", "you have")) \
        .withColumn(cols, regexp_replace(cols, r"it\x89Ûªs", "it is")) \
        .withColumn(cols, regexp_replace(cols, r"doesn\x89Ûªt", "does not")) \
        .withColumn(cols, regexp_replace(cols, r"It\x89Ûªs", "It is")) \
        .withColumn(cols, regexp_replace(cols, r"Here\x89Ûªs", "Here is")) \
        .withColumn(cols, regexp_replace(cols, r"I\x89Ûªve", "I have")) \
        .withColumn(cols, regexp_replace(cols, r"y'all", "you all")) \
        .withColumn(cols, regexp_replace(cols, r"can\x89Ûªt", "cannot")) \
        .withColumn(cols, regexp_replace(cols, r"wouldn\x89Ûªt", "would not")) \
        .withColumn(cols, regexp_replace(cols, r"Y'all", "You all")) \
        .withColumn(cols, regexp_replace(cols, r"that\x89Ûªs", "that is")) \
        .withColumn(cols, regexp_replace(cols, r"You\x89Ûªre", "You are")) \
        .withColumn(cols, regexp_replace(cols, r"where's", "where is")) \
        .withColumn(cols, regexp_replace(cols, r"Don\x89Ûªt", "Do not")) \
        .withColumn(cols, regexp_replace(cols, r"Can\x89Ûªt", "Cannot")) \
        .withColumn(cols, regexp_replace(cols, r"you\x89Ûªll", "you will")) \
        .withColumn(cols, regexp_replace(cols, r"I\x89Ûªd", "I would")) \
        .withColumn(cols, regexp_replace(cols, r"Ain't", "am not")) \
        .withColumn(cols, regexp_replace(cols, r"youve", "you have")) \
        .withColumn(cols, regexp_replace(cols, r"donå«t", "do not")) \
        .withColumn(cols, regexp_replace(cols, r"some1", "someone")) \
        .withColumn(cols, regexp_replace(cols, r"yrs", "years")) \
        .withColumn(cols, regexp_replace(cols, r"hrs", "hours")) \
        .withColumn(cols, regexp_replace(cols, r"2morow|2moro", "tomorrow")) \
        .withColumn(cols, regexp_replace(cols, r"2day", "today")) \
        .withColumn(cols, regexp_replace(cols, r"lmao|lolz|rofl", "lol")) \
        .withColumn(cols, regexp_replace(cols, r"4got|4gotten", "forget")) \
        .withColumn(cols, regexp_replace(cols, r"thanx|thnx", "thanks"))
    return df


def replace_repeating_characters(df: pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    # Replace repeating characters, for example, loveeeee into lovee - more than 2 of the same
    return df.withColumn(cols, regexp_replace(cols, r'(.)\1{2,}', r'\1\1'))


def replace_punctuation(df:pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    df = df.withColumn(cols, regexp_replace(cols,'[^\sa-zA-Z0-9]', ' '))
    return df

def replace_specific_characters(df:pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    df = df.withColumn(cols, regexp_replace(cols, u'\u201c', " ")) \
        .withColumn(cols, regexp_replace(cols, u'\u201d', ' ')) \
        .withColumn(cols, regexp_replace(cols, u'\u2014', ' ')) \
        .withColumn(cols, regexp_replace(cols, u'\u2013', ' ')) \
        .withColumn(cols, regexp_replace(cols, u'\u2026', ' '))
    return df

