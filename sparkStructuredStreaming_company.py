from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sparkStructuredStreaming
import sys
import numpy as np
import datetime

# run with 
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar sparkStructuredStreaming_company.py "127.0.0.1:9092"...
# arg = "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)
 
bootstrap = sys.argv[1]

spark = SparkSession \
            .builder \
            .appName("KafkaIEXStructuredStreaming") \
            .master("local[*]") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp") \
            .getOrCreate()

sss = sparkStructuredStreaming.kafka_spark_stream(bootstrap)

parsedDF = sss.stream_company(spark)
    
selectDF = parsedDF \
        .select(explode(array("company_data")))\
        .select("col.*")
                
#sss.write_console(selectDF)
sss.write_es(selectDF,"symbol","company")

spark.streams.awaitAnyTermination()