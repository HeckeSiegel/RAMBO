from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import sys
import numpy as np
import datetime

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

parsedDF = sss.stream_quotes(spark)

'''selectDF_hdfs = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*")\
        .dropDuplicates(["symbol", "latestPrice", "latestTime"])'''

selectDF_es = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*",get_datetime("col.latestUpdate").cast("String").alias("date"))
      
#sss.write_hdfs(selectDF_hdfs,hdfs_path, output_dir) 
#sss.write_console(selectDF)
sss.write_es(selectDF_es,"latestUpdate","quotes")

spark.streams.awaitAnyTermination()



 
                
        
        
        
        
        
        
        
        
        