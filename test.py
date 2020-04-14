import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 
    
spark = SparkSession \
        .builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "file:///C:/temp") \
        .getOrCreate()
        
        
x = '[{"a": "123", "b" : "456"},{"a": "789", "b" : "101112"}]'
d = [{'key' : 'AAP', 'value' : x}]
df = spark.createDataFrame(d)
df.printSchema()
df.show()

max_json_parts = 10
#json_elements = [get_json_object(df.value, '$[%d]' %i) for i in range(10)]
#df1 = df.select(df.key, explode(array(get_json_object(df.value, '$[%d]' %i) for i in range(10))))
df1 = df.select(df.key, explode(array(get_json_object(df.value, '$[0]'), get_json_object(df.value, '$[1]'))))
df1.printSchema()
df1.show()

schema = StructType() \
        .add("a", StringType())\
        .add("b",StringType())

df2 = df1.select(df1.key, from_json(df1.col,schema).alias("data"))
df2.printSchema()
df2.show()

df3 = df2.select(explode(array("data"))).select("col.a","col.b")
df3.printSchema()
df3.show()



