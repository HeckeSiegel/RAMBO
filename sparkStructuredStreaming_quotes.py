from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# run from anaconda shell with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkStructuredStreaming_quotes.py "127.0.0.1:9092"
topic = "iex_json_quotes" 
bootstrap = sys.argv[1]

# bootstrap = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
        
spark = SparkSession \
        .builder \
        .appName("KafkaIEXStructuredStreaming") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .getOrCreate()
        
# Streaming DataFrame that reads from topic (args[0])
streamingDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
        
streamingDF.printSchema()

# Parse key and value columns (JSON)
# create expected schema for JSON data to convert binary value into SparkSQL struct

schema = StructType() \
        .add("symbol", StringType())\
        .add("companyName",StringType())\
        .add("primaryExchange", StringType())\
        .add("calculationPrice", StringType())\
        .add("open", DoubleType())\
        .add("openTime", LongType())\
        .add("close",DoubleType())\
        .add("closeTime", LongType())\
        .add("high", DoubleType())\
        .add("low", DoubleType())\
        .add("volume",LongType())\
        .add("latestPrice", DoubleType())\
        .add("latestSource", StringType())\
        .add("latestTime", StringType())\
        .add("latestUpdate", LongType())\
        .add("latestVolume", LongType())\
        .add("delayedPrice", DoubleType())\
        .add("delayedPriceTime", LongType())\
        .add("extendedPrice", DoubleType())\
        .add("extendedChange", DoubleType())\
        .add("extendedChangePercent", DoubleType())\
        .add("extendedPriceTime", LongType())\
        .add("previousClose", DoubleType())\
        .add("previousVolume", LongType())\
        .add("change", DoubleType())\
        .add("changePercent", DoubleType())\
        .add("avgTotalVolume", LongType())\
        .add("marketCap", LongType())\
        .add("peRatio", DoubleType())\
        .add("week52High", DoubleType())\
        .add("week52Low", DoubleType())\
        .add("ytdChange", DoubleType())\
        .add("lastTradeTime", LongType())\
        .add("isUSMarketOpen", BooleanType())\
        
parsedDF = streamingDF\
        .select(col("key").cast("String"), from_json(col("value").cast("string"), schema).alias("quote_data"))     

parsedDF.printSchema()

selectDF = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.companyName","col.primaryExchange","col.close","col.marketCap")
        
selectDF.printSchema()
        
# Write into console
writeDF = selectDF \
        .writeStream \
        .queryName("select_quotes") \
        .trigger(processingTime='1 seconds') \
        .outputMode("append") \
        .format("console") \
        .start()

            
# Run forever until terminated        
writeDF.awaitTermination()       
     
        
        
        
        
        
        
        
        
        