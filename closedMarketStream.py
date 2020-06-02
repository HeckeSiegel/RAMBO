from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from lib import sparkStructuredStreaming
import sys
import numpy as np
import datetime
from pyspark.sql.window import Window
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
"""
Spark Stream to simulate an open market
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar closedMarketStream.py 
Replace "C:" with the path to your elasticsearch-hadoop directory
"""

#get_datetime = udf(lambda x : datetime.datetime.fromtimestamp((x-7200000)/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))
simulate_time = F.udf(lambda x : (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d"'T'"%H:%M:%S"))
simulate_price = F.udf(lambda x : np.random.uniform(-x*0.01,x*0.01)+x)

get_id_now = F.udf(lambda x : datetime.datetime.now().strftime("%Y%m%d"))
get_datetime = F.udf(lambda x : datetime.datetime.fromtimestamp((x-7200000)/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))
       
# initialize spark session and define hdfs path to write into 
bootstrap = "127.0.0.1:9092"

hdfs_path = "hdfs://0.0.0.0:19000"
output_dir = "realtimeSB"

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreamingClosedMarket") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

# stream for all topics
parsedDF_news = sss.stream_news(spark)
parsedDF_quotes = sss.stream_quotes(spark)
parsedDF_company = sss.stream_company(spark)

# quotes
selectDF_quotes = parsedDF_quotes \
        .select(F.explode(F.array("quote_data")))\
        .select("col.*")
        
selectDF_quotes_hdfs = selectDF_quotes.select(simulate_time("latestUpdate").cast("Timestamp").alias("Datetime"),simulate_price("latestPrice").cast("float").alias("latestPrice"),"symbol")
selectDF_quotes_es = selectDF_quotes.select(get_id_now("latestUpdate").alias("depotid"), simulate_time("latestUpdate").cast("String").alias("date"),simulate_price("latestPrice").alias("latestPrice"),"symbol")

# news
selectDF_news = parsedDF_news \
        .select(F.explode(F.array("news_data")))\
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

sentiment_analysis_udf = F.udf(sentiment_analysis, FloatType())
##############################################################################

selectDF_news = selectDF_news.withColumn("sentiment_score",sentiment_analysis_udf(selectDF_news['headline'],selectDF_news['summary']))\
        .withColumn("date",simulate_time("datetime").cast("String")) \
        .withColumnRenamed("related","symbol") \
        .withColumn("depotid", get_id_now("datetime"))
        
# company
selectDF_company = parsedDF_company \
        .select(F.explode(F.array("company_data")))\
        .select("col.*")\
        .withColumn("depotid", get_id_now("symbol"))\
        .withColumn("date",simulate_time("symbol").cast("String"))\
        .withColumn("esid",F.concat("symbol","depotid"))

# write into hdfs     
sss.write_hdfs(selectDF_quotes_hdfs,hdfs_path, output_dir)

# write into elasticsearch
sss.write_es(selectDF_news,"date","newssb")
sss.write_es(selectDF_quotes_es,"date","quotessb")
sss.write_es(selectDF_company,"esid","companysb")

# write into console
#sss.write_console(selectDF)

spark.streams.awaitAnyTermination()
       