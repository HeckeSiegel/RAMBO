from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import sys
import sparkStructuredStreaming
import numpy as np

# run with spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 
# --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar
# bootstrap = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
bootstrap = sys.argv[1]

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF_quotes = sss.stream_quotes(spark)       
parsedDF_news = sss.stream_news(spark) 

# udf to convert epoch time to human readable german time
get_time = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%H:%M:%S"))
get_date = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"))

#use this for elasticsearch, otherwise it won't recognize date field
get_datetime = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"'T'"%H:%M:%S")) 
      
selectDF_quotes = parsedDF_quotes \
        .select(explode(array("quote_data")))\
        .select("col.*",get_datetime("col.latestUpdate").cast("String").alias("date"))

selectDF_news = parsedDF_news \
        .select(explode(array("news_data")))\
        .select("col.*",get_datetime("col.datetime").cast("String").alias("date"))\
        .dropna()\
        .withColumnRenamed("related","symbol")

#index and type are the same word because elasticsearch will get rid of type in
#future versions        
path_news = "news/news"

#random number for checkpoint location because it has to be different for each
#stream (there's probably a smarter way to solve this)      
rand_news = str(np.random.randint(1,1000000))

selectDF_news.writeStream\
    .option("checkpointLocation","checkpoint/"+rand_news)\
    .option("es.mapping.id", "datetime")\
    .outputMode("append")\
    .format("es")\
    .start(path_news)
    
path_quotes = "quotes/quotes"            
rand_quotes = str(np.random.randint(1,1000000))

selectDF_quotes.writeStream\
    .option("checkpointLocation","checkpoint/"+rand_quotes)\
    .option("es.mapping.id", "latestUpdate")\
    .outputMode("append")\
    .format("es")\
    .start(path_quotes)
    
spark.streams.awaitAnyTermination()
    
