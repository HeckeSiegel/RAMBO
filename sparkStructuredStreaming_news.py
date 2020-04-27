from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar sparkStructuredStreaming_quotes.py "127.0.0.1:9092"...
# arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)

#use this for elasticsearch, otherwise it won't recognize date field
get_datetime = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))
 
bootstrap = sys.argv[1]
hdfs_path = "hdfs://0.0.0.0:19000"
output_dir = "iex/quotes/<date>"

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = sss.stream_news(spark)

'''selectDF_hdfs = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.*")\
        .dropna().dropDuplicates(["headline"])      '''  

selectDF_es = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.*",get_datetime("col.datetime").cast("String").alias("date")) \
        .withColumnRenamed("related","symbol")

# sentiment analysis of news
sia = SentimentIntensityAnalyzer()

def sentiment_analysis(text):
    return sia.polarity_scores(text)['compound']

sentiment_analysis_udf = udf(sentiment_analysis, FloatType())

selectDF_es = selectDF_es.withColumn("sentiment_score",sentiment_analysis_udf(selectDF_es['summary']))
        
#writeDF_hdfs = sss.write_hdfs(selectDF,hdfs_path, output_dir)        
writeDF_console = sss.write_console(selectDF_es)
#sss.write_es(selectDF_es,"datetime","news")

spark.streams.awaitAnyTermination()
        
  
        
