from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import sys
import sparkStructuredStreaming

# bootstrap = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
bootstrap = sys.argv[1]
hdfs_path = sys.argv[2]
output_dir = sys.argv[3]

# udf to convert epoch time to spark TimestampType
get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = sss.stream_quotes(spark)       

selectDF = parsedDF \
        .select(explode(array("quote_data")))\
        .select(get_timestamp("col.latestUpdate").cast("timestamp").alias("timestamp"),"col.latestPrice")\
        .withWatermark("timestamp", "24 hours")

def time_chart(df,interval):
    # use df with "timestamp", "latestPrice", "Watermark"
    # get open, high, low prices for each intervall to make candlechart
    interval_values = df.groupBy(
        window(df.timestamp, interval))\
        .agg(max("latestPrice").alias("high"),\
            min("latestPrice").alias("low"),\
            min("timestamp").alias("open_time"))\
        .select("window.start","window.end","high","low","open_time")\
        .withWatermark("start", interval)
    
    # join to get opening price from opening time
    chart = interval_values.join(df,interval_values.open_time == df.timestamp, "left")\
        .drop("open_time","timestamp")\
        .withColumnRenamed("latestPrice","open")
        
    return chart

selectDF_tick = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.latestVolume","col.latestPrice")\
        .withWatermark("timestamp", "24 hours")


#writeDF_hdfs = sss.write_hdfs(selectDF,hdfs_path, output_dir)        
writeDF_console = sss.write_console(chart)

spark.streams.awaitAnyTermination()
