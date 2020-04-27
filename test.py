import nltk
import warnings
import csv
import pandas as pd
warnings.filterwarnings('ignore')
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys

#nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

# update lexicon
# stock market lexicon
'''stock_lex = pd.read_csv('lexicon_data/stock_lex.csv')
stock_lex['sentiment'] = (stock_lex['Aff_Score'] + stock_lex['Neg_Score'])/2
stock_lex = dict(zip(stock_lex.Item, stock_lex.sentiment))
stock_lex = {k:v for k,v in stock_lex.items() if len(k.split(' '))==1}
stock_lex_scaled = {}
for k, v in stock_lex.items():
    if v > 0:
        stock_lex_scaled[k] = v / max(stock_lex.values()) * 4
    else:
        stock_lex_scaled[k] = v / min(stock_lex.values()) * -4

# Loughran and McDonald
positive = []
with open('lexicon_data/lm_positive.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        positive.append(row[0].strip())
    
negative = []
with open('lexicon_data/lm_negative.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        entry = row[0].strip().split(" ")
        if len(entry) > 1:
            negative.extend(entry)
        else:
            negative.append(entry[0])

final_lex = {}
final_lex.update({word:2.0 for word in positive})
final_lex.update({word:-2.0 for word in negative})
final_lex.update(stock_lex_scaled)
final_lex.update(sia.lexicon)
sia.lexicon = final_lex'''

#Initialize spark stream
#use this for elasticsearch, otherwise it won't recognize date field
get_datetime = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))

def sentiment_analysis(text):
    return sia.polarity_scores(text)['compound']

sentiment_analysis_udf = udf(sentiment_analysis, FloatType())

bootstrap = sys.argv[1]

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = sss.stream_news(spark)

selectDF = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.*",get_datetime("col.datetime").cast("String").alias("date")) \
        .withColumnRenamed("related","symbol")

selectDF = selectDF.withColumn("sentiment_score_headline",sentiment_analysis_udf(selectDF['headline']))
selectDF = selectDF.withColumn("sentiment_score_summary",sentiment_analysis_udf(selectDF['summary']))

#writeDF_console = sss.write_console(selectDF)
sss.write_es(selectDF,"datetime","news")

spark.streams.awaitAnyTermination()
