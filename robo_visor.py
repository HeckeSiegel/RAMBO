from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib import sparkStructuredStreaming
import sys
import numpy as np
import datetime
from pyspark.sql.window import Window

""" 
Spark Stream for robo visor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar robo_visor.py arg
arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
Replace "C:" with the path to your elasticsearch-hadoop directory
"""

get_datetime = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
get_datetime_minute = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M"))
               
# initialize spark session and define hdfs path to write into 
bootstrap = sys.argv[1]
mom = "10 minutes"
hdfs_path = "hdfs://0.0.0.0:19000"
output_dir_bnh = "realtime/buyAndHold"
output_dir_momentum = "realtime/momentum"

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

# stream from quotes topic
parsedDF = sss.stream_quotes(spark)

selectDF = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*")
        
selectDF_bnh = selectDF.select(get_datetime("latestUpdate").cast("Timestamp").alias("Datetime"),"latestPrice","symbol")

selectDF_mom = selectDF.select(get_datetime_minute("latestUpdate").cast("Timestamp").alias("Datetime"),"latestPrice","symbol")

# write into hdfs     
sss.write_hdfs(selectDF_bnh,hdfs_path, output_dir_bnh)
sss.write_hdfs(selectDF_mom,hdfs_path, output_dir_momentum)
#sss.write_console(selectDF_mom)

spark.streams.awaitAnyTermination()



 
                
        
        
        
        
        
        
        
        
        