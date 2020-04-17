from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkStructuredStreaming_quotes.py <arg>
# arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
 
bootstrap = sys.argv[1]

streamingDF = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = streamingDF.stream_news()

selectDF = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.datetime","col.headline","col.source","col.summary","col.related")\
        .dropna().dropDuplicates(["headline"])

# convert unix timestamp to human readable
get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))        
#selectDF = selectDF.withColumn('datetime_hr', get_timestamp(selectDF.datetime))        
        
#writeDF_hdfs = streamingDF.write_hdfs(selectDF,"hdfs://0.0.0.0:19000/tmp7", "hdfs://0.0.0.0:19000/test7","datetime")        
writeDF_console = streamingDF.write_console(selectDF)

#writeDF_hdfs.awaitTermination()
writeDF_console.awaitTermination()
        
  
        
        
        
        
        
        
        
        
        