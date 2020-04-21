from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import datetime
import sys

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkStructuredStreaming_news.py <arg1>...
# arg1 = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
# arg2 = "hdfs://0.0.0.0:19000" (local)
# arg3 = "iex/news/<date>"
 
bootstrap = sys.argv[1]
hdfs_path = sys.argv[2]
output_dir = sys.argv[3]

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
        .select("col.*")\
        .dropna().dropDuplicates(["headline"])

# convert unix timestamp to human readable
get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))        
#selectDF = selectDF.withColumn('datetime_hr', get_timestamp(selectDF.datetime))        
        
writeDF_hdfs = sss.write_hdfs(selectDF,hdfs_path, output_dir)        
writeDF_console = sss.write_console(selectDF)

spark.streams.awaitAnyTermination()
        
  
        
        
        
        
        
        
        
        
        