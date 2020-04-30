from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar elasticSearch.py "127.0.0.1:9092"...
# arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)

#use this for elasticsearch, otherwise it won't recognize date field
get_datetime = udf(lambda x : datetime.datetime.fromtimestamp((x-7200000)/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))

# initialize spark session 
bootstrap = sys.argv[1]

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

# stream from all 3 topics
parsedDF_news = sss.stream_news(spark)
parsedDF_quotes = sss.stream_quotes(spark)
parsedDF_company = sss.stream_company(spark)

selectDF_news = parsedDF_news \
        .select(explode(array("news_data")))\
        .select("col.*") \
        .dropna()
        
##################### sentiment analysis of news #############################
sia = SentimentIntensityAnalyzer()
#update lexicon
with open('lexicon_data/final_lex.json', 'r') as fp:
    final_lex = json.load(fp)
    
final_lex.update(sia.lexicon)
sia.lexicon = final_lex

def sentiment_analysis(txt1,txt2):
    text = txt1 + ' ' + txt2
    return sia.polarity_scores(text)['compound']

sentiment_analysis_udf = udf(sentiment_analysis, FloatType())
##############################################################################

selectDF_news = selectDF_news.withColumn("sentiment_score",sentiment_analysis_udf(selectDF_news['headline'],selectDF_news['summary']))\
        .withColumn("date",get_datetime("datetime").cast("String")) \
        .withColumnRenamed("related","symbol")
        
selectDF_quotes = parsedDF_quotes \
        .select(explode(array("quote_data")))\
        .select("col.*",get_datetime("col.latestUpdate").cast("String").alias("date"))
        
selectDF_company = parsedDF_company \
        .select(explode(array("company_data")))\
        .select("col.*")
        
# write into elasticsearch
sss.write_es(selectDF_news,"datetime","news")
sss.write_es(selectDF_quotes,"latestUpdate","quotes")
sss.write_es(selectDF_company,"symbol","company")

spark.streams.awaitAnyTermination()



