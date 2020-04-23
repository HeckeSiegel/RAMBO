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
get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
           
selectDF_quotes = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*",get_timestamp("col.latestUpdate").cast("timestamp").alias("timestamp"))
        
selectDF_quotes = selectDF_quotes.withColumn("timestamp_str",col("timestamp").cast("String"))

selectDF_quotes = selectDF_quotes.select("*",window(selectDF_quotes.timestamp,"1 minutes"))

selectDF_quotes = selectDF_quotes.withColumn("window.timestamp",col("window.start").cast("String"))

#sss.write_console(selectDF_window).awaitTermination()

path = "quotes/quotes"            

selectDF_quotes.writeStream\
    .option("checkpointLocation",path + "/checkpoint")\
    .outputMode("append")\
    .format("es")\
    .start(path)\
    .awaitTermination()
    
