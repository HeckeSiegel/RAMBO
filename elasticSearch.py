from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import sys
import sparkStructuredStreaming


# run with '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 pyspark-shell'
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

parsedDF = sss.stream_quotes(spark)       

# udf to convert epoch time to spark TimestampType
get_time = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%H:%M:%S"))
get_date = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d"))
       
selectDF_quotes = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*",get_time("col.latestUpdate").cast("String").alias("time")\
                ,get_date("col.latestUpdate").cast("String").alias("date"))

path = "quotes/quotes"            

selectDF_quotes.writeStream\
    .option("checkpointLocation","checkpoint")\
    .outputMode("append")\
    .format("es")\
    .start(path)\
    .awaitTermination()
    
