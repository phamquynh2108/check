import pyspark.sql.streaming
import pyspark.sql.functions as F
import torch
import numpy as np
from nltk import pos_tag, word_tokenize
from variable import *
def decontract_text(df: pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    # specific
    df = df.withColumn(cols, F.regexp_replace(cols, "\n", " ")) \
        .withColumn(cols, F.regexp_replace(cols, '&amp;', ' and ')) \
        .withColumn(cols, F.regexp_replace(cols, r'@.*?( |$)', 'USERTAG ')) \
        .withColumn(cols, F.regexp_replace(cols, r'http[s]{0,1}.*?( |$)', 'URLTAG ')) \
        .withColumn(cols, F.regexp_replace(cols, r'#', '')) \
        .withColumn(cols, F.regexp_replace(cols, r"won't", "will not")) \
        .withColumn(cols, F.regexp_replace(cols, r"won't", "will not")) \
        .withColumn(cols, F.regexp_replace(cols, r"can\'t", "can not")) \
        .withColumn(cols, F.regexp_replace(cols, r"n\'t", " not")) \
        .withColumn(cols, F.regexp_replace(cols, r"n\u2019t", " not")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'re", " are")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'s", " is")) \
        .withColumn(cols, F.regexp_replace(cols, r"\u2019s", " is")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'d", " would")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'ll", " will")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'t", " not")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'ve", " have")) \
        .withColumn(cols, F.regexp_replace(cols, r"\'m", " am")) \
        .withColumn(cols, F.regexp_replace(cols, r"don\x89Ûªt", "do not")) \
        .withColumn(cols, F.regexp_replace(cols, r"I\x89Ûªm", "I am")) \
        .withColumn(cols, F.regexp_replace(cols, r"you\x89Ûªve", "you have")) \
        .withColumn(cols, F.regexp_replace(cols, r"it\x89Ûªs", "it is")) \
        .withColumn(cols, F.regexp_replace(cols, r"doesn\x89Ûªt", "does not")) \
        .withColumn(cols, F.regexp_replace(cols, r"It\x89Ûªs", "It is")) \
        .withColumn(cols, F.regexp_replace(cols, r"Here\x89Ûªs", "Here is")) \
        .withColumn(cols, F.regexp_replace(cols, r"I\x89Ûªve", "I have")) \
        .withColumn(cols, F.regexp_replace(cols, r"y'all", "you all")) \
        .withColumn(cols, F.regexp_replace(cols, r"can\x89Ûªt", "cannot")) \
        .withColumn(cols, F.regexp_replace(cols, r"wouldn\x89Ûªt", "would not")) \
        .withColumn(cols, F.regexp_replace(cols, r"Y'all", "You all")) \
        .withColumn(cols, F.regexp_replace(cols, r"that\x89Ûªs", "that is")) \
        .withColumn(cols, F.regexp_replace(cols, r"You\x89Ûªre", "You are")) \
        .withColumn(cols, F.regexp_replace(cols, r"where's", "where is")) \
        .withColumn(cols, F.regexp_replace(cols, r"Don\x89Ûªt", "Do not")) \
        .withColumn(cols, F.regexp_replace(cols, r"Can\x89Ûªt", "Cannot")) \
        .withColumn(cols, F.regexp_replace(cols, r"you\x89Ûªll", "you will")) \
        .withColumn(cols, F.regexp_replace(cols, r"I\x89Ûªd", "I would")) \
        .withColumn(cols, F.regexp_replace(cols, r"Ain't", "am not")) \
        .withColumn(cols, F.regexp_replace(cols, r"youve", "you have")) \
        .withColumn(cols, F.regexp_replace(cols, r"donå«t", "do not")) \
        .withColumn(cols, F.regexp_replace(cols, r"some1", "someone")) \
        .withColumn(cols, F.regexp_replace(cols, r"yrs", "years")) \
        .withColumn(cols, F.regexp_replace(cols, r"hrs", "hours")) \
        .withColumn(cols, F.regexp_replace(cols, r"2morow|2moro", "tomorrow")) \
        .withColumn(cols, F.regexp_replace(cols, r"2day", "today")) \
        .withColumn(cols, F.regexp_replace(cols, r"lmao|lolz|rofl", "lol")) \
        .withColumn(cols, F.regexp_replace(cols, r"4got|4gotten", "forget")) \
        .withColumn(cols, F.regexp_replace(cols, r"thanx|thnx", "thanks"))
    return df


def replace_repeating_characters(df: pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    # Replace repeating characters, for example, loveeeee into lovee - more than 2 of the same
    return df.withColumn(cols, F.regexp_replace(cols, r'(.)\1{2,}', r'\1\1'))


def replace_punctuation(df:pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    df = df.withColumn(cols, F.regexp_replace(cols,'[^\sa-zA-Z0-9]', ' '))
    return df

def replace_specific_characters(df:pyspark.sql.DataFrame, cols: str) -> pyspark.sql.DataFrame:
    df = df.withColumn(cols, F.regexp_replace(cols, u'\u201c', " ")) \
        .withColumn(cols, F.regexp_replace(cols, u'\u201d', ' ')) \
        .withColumn(cols, F.regexp_replace(cols, u'\u2014', ' ')) \
        .withColumn(cols, F.regexp_replace(cols, u'\u2013', ' ')) \
        .withColumn(cols, F.regexp_replace(cols, u'\u2026', ' '))
    return df

    # output_dir = './outputs'
    # tokenizer = BertTokenizer.from_pretrained(output_dir)
    # model_loaded = BertForSequenceClassification.from_pretrained(output_dir)
def Sentiment(tokenizer,model_loaded,sent):
    encoded_dict = tokenizer.encode_plus(
        sent,
        add_special_tokens=True,
        max_length=100,
        pad_to_max_length=True,
        return_attention_mask=True,
        return_tensors='pt',
    )
    input_id = encoded_dict['input_ids']

    attention_mask = encoded_dict['attention_mask']
    input_id = torch.LongTensor(input_id)
    attention_mask = torch.LongTensor(attention_mask)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model_loaded = model_loaded.to(device)
    input_id = input_id.to(device)
    attention_mask = attention_mask.to(device)

    with torch.no_grad():
        outputs = model_loaded(input_id, token_type_ids=None, attention_mask=attention_mask)

    logits = outputs[0]
    ans = logits.argmax()
    # proba
    logits = logits.cpu().numpy()[0]
    expl = np.exp(logits)
    sumExpL = sum(expl)
    result = []
    for i in expl:
        result.append((i * 1.0)/sumExpL)
    pred = str(max(result))
    label = 'Pos' if ans == 1 else 'Neg'

    return [pred, label]

def get_wordcloud(text):
    x = [word for word, pos in pos_tag(word_tokenize(text)) if pos.startswith('JJ') and word not in ['usertag','urltag']]
    y = filter_list +['airpods', 'ipod']
    b =[i for i in y if i in text.lower()]
    result = list(set(x+b))
    return result
