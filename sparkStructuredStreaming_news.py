from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json

""" 
This script streams only from news topic. Run it from the command line with
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar sparkStructuredStreaming_news.py arg
arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
"""

#use this for elasticsearch, otherwise it won't recognize date field
get_datetime = udf(lambda x : datetime.datetime.fromtimestamp((x-7200000)/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S"))

#for hdfs to partition the data by date 
get_date = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"))

# initialize spark session and define hdfs path to write into 
bootstrap = sys.argv[1]
hdfs_path = "hdfs://0.0.0.0:19000"
output_dir = "iex/news"

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

# stream from news topic
parsedDF = sss.stream_news(spark)

# drop na before transforming time, otherwise will get error
# drop duplicates is only necessary for hdfs, elasticsearch does it by itself
# because we give a unique id
selectDF = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.*")\
        .dropna().dropDuplicates(["headline"])

selectDF_hdfs = selectDF.withColumn("date",get_date("datetime").cast("Timestamp"))

selectDF_es = selectDF.withColumn("date",get_datetime("col.datetime").cast("String")) \
        .withColumnRenamed("related","symbol")

######################## sentiment analysis of news ############################
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

selectDF_es = selectDF_es.withColumn("sentiment_score",sentiment_analysis_udf(selectDF_es['headline'],selectDF_es['summary']))

###############################################################################

# write streams either into hdfs, console, es or all at once        
writeDF_hdfs = sss.write_hdfs(selectDF_hdfs,hdfs_path, output_dir, "date")        
#writeDF_console = sss.write_console(selectDF_es)
#sss.write_es(selectDF_es,"datetime","news")

spark.streams.awaitAnyTermination()