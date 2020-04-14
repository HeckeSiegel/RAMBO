from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
import sys

# run from anaconda shell with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkStructuredStreaming_news.py "127.0.0.1:9092"
topic = "iex_json_news" 
bootstrap = sys.argv[1]

# bootstrap = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
        
spark = SparkSession \
        .builder \
        .appName("KafkaIEXStructuredStreamingNews") \
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

parsedDF = streamingDF.select(col("key").cast("String"), col("value").cast("string"))

parsedDF = parsedDF.select(parsedDF.key,\
                               explode(array(get_json_object(parsedDF.value, '$[0]'),\
                                             get_json_object(parsedDF.value, '$[1]'),\
                                             get_json_object(parsedDF.value, '$[2]'),\
                                             get_json_object(parsedDF.value, '$[3]'),\
                                             get_json_object(parsedDF.value, '$[4]'),\
                                             get_json_object(parsedDF.value, '$[5]'),\
                                             get_json_object(parsedDF.value, '$[6]'),\
                                             get_json_object(parsedDF.value, '$[7]'),\
                                             get_json_object(parsedDF.value, '$[8]'),\
                                             get_json_object(parsedDF.value, '$[9]'))))

schema = StructType() \
        .add("datetime", LongType())\
        .add("headline",StringType())\
        .add("source", StringType())\
        .add("url", StringType())\
        .add("summary", StringType())\
        .add("related", StringType())\
        .add("image",StringType())\
        .add("lang", StringType())\
        .add("hasPaywall", BooleanType())
        
parsedDF = parsedDF\
        .select(parsedDF.key, from_json(parsedDF.col, schema).alias("news_data"))    
        
parsedDF.printSchema()

selectDF = parsedDF \
        .select(explode(array("news_data")))\
        .select("col.datetime","col.headline","col.related")#\
#        .dropna().dropDuplicates(["headline"])

# convert unix timestamp to human readable
#get_timestamp = udf(lambda x : datetime.datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))        
#selectDF = selectDF.withColumn('datetime_hr', get_timestamp(selectDF.datetime))        
selectDF.printSchema()
        
# Write into console
writeDF = selectDF \
        .writeStream \
        .queryName("select_news") \
        .trigger(processingTime='1 seconds') \
        .outputMode("append") \
        .format("console") \
        .start()
     
writeDF.awaitTermination()      
        
  
        
        
        
        
        
        
        
        
        