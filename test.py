from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import sparkStructuredStreaming

# bootstrap = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)

streamingDF = sparkStructuredStreaming.kafka_spark_stream("127.0.0.1:9092")
parsedDF_quotes = streamingDF.stream_quotes()
parsedDF_news = streamingDF.stream_news()

parsedDF_quotes.printSchema()
parsedDF_news.printSchema()

selectDF_quotes = parsedDF_quotes \
        .select(explode(array("quote_data")))\
        .select("col.companyName","col.primaryExchange","col.latestPrice","col.latestTime")\
        .dropDuplicates(["companyName", "latestPrice", "latestTime"])

        
selectDF_news = parsedDF_news \
        .select(explode(array("news_data")))\
        .select("col.datetime","col.headline","col.related")\
        .dropna().dropDuplicates(["headline"])
                

#writeDF_quotes = streamingDF.write_hdfs(selectDF_quotes,"hdfs://0.0.0.0:19000/tmp3", "hdfs://0.0.0.0:19000/test3/","companyName")        
writeDF_news = streamingDF.write_console(selectDF_news)
writeDF_news.awaitTermination()