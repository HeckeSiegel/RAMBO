from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import sys

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 sparkStructuredStreaming_quotes.py <arg>
# arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
 
bootstrap = sys.argv[1]

streamingDF = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = streamingDF.stream_quotes()

selectDF = parsedDF \
        .select(explode(array("quote_data")))\
        .select("col.companyName","col.primaryExchange","col.latestPrice","col.latestTime")\
        .dropDuplicates(["companyName", "latestPrice", "latestTime"])
        
writeDF_hdfs = streamingDF.write_hdfs(selectDF,"hdfs://0.0.0.0:19000/tmp6", "hdfs://0.0.0.0:19000/test6","companyName")        
#writeDF_console = streamingDF.write_console(selectDF_quotes)

writeDF_hdfs.awaitTermination()
#writeDF_console.awaitTermination()


 
                
        
        
        
        
        
        
        
        
        