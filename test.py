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
#hdfs_path = sys.argv[2]
#output_dir = sys.argv[3]

# udf to convert epoch time to spark TimestampType
get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
watermark = "20 hours"

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
        .select(get_timestamp("col.latestUpdate").cast("timestamp").alias("timestamp"),"col.latestPrice","col.symbol")\
        .withWatermark("timestamp", watermark)

selectDF2 = parsedDF \
        .select(explode(array("quote_data")))\
        .select(get_timestamp("col.latestUpdate").cast("timestamp").alias("timestamp"),"col.latestPrice","col.symbol")

selectDF2 = selectDF2\
            .select(window(selectDF2.timestamp, "1 minutes"), selectDF2.latestPrice)\
            .select(col("window.start").alias("timestamp"),"latestPrice")
            
selectDF3 = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.*")\
        .withColumnRenamed("latestUpdate","timestamp")
        
def time_chart(df,interval):
    # use df with "timestamp", "latestPrice", "Watermark"
    # get open, high, low prices for each time interval
    interval_values = df.groupBy(
        window(df.timestamp, interval))\
        .agg(max("latestPrice").alias("high"),\
            min("latestPrice").alias("low"),\
            min("timestamp").alias("open_time"))\
        .select("window.start","window.end","high","low","open_time")\
        .withWatermark("start", watermark)
    
    # join to get opening price from opening time
    chart = interval_values.join(df,interval_values.open_time == df.timestamp, "left")\
        .drop("open_time","timestamp")\
        .withColumnRenamed("latestPrice","open")
        
    return chart

def moving_average(spark, df, update, interval):
    # simple moving average for the interval "interval"
    
    windowdf = df.select(window(df.timestamp, interval, update), df.latestPrice)
    
    windowdf.createOrReplaceTempView("windowdf_sql")
    
    sma = spark.sql("""SELECT windowdf_sql.window.start AS time, avg(windowdf_sql.latestPrice) AS average
                    FROM windowdf_sql
                    Group BY windowdf_sql.window
                    """)   
    return sma

  
#chart = time_chart(selectDF, "5 minutes")
#average = moving_average(spark,selectDF, "1 minutes", "8 minutes")
path = "quotes/2020-04-22"
selectDF3.writeStream\
    .option("checkpointLocation",path+"/checkpoint")\
    .outputMode("append")\
    .format("es")\
    .start(path)\
    .awaitTermination()
    
#sss.write_console(selectDF2).awaitTermination()

























