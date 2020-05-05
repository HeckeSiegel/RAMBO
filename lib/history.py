import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from pytz import timezone

class history:
    
        
    def to_es(self,symbol,interval,period,sqlContext):
        """
        Writes historical stock data from yahoo finance into elasticsearch

        Parameters
        ----------
        symbol : Stock ticker symbol from US market
        interval : 1m,2m,5m,15m,30m,60m,90m,1d,5d,1wk,1mo,3mo
        period : 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        sqlContext : sqlContext from SparkSession.

        Returns
        -------
        None.

        """
        #udf that transforms timestamp into the right format for es
        get_datetime_yahoo = udf(lambda x : ((timezone('Europe/Berlin').localize(x)).astimezone(timezone('UTC'))).strftime("%Y-%m-%d"'T'"%H:%M:%S"))
        
        #read historical data from yahoo finance into a pandas df
        ticker = yf.Ticker(symbol)
        pandas_history = ticker.history(period=period, interval=interval)
        
        #tranform into spark df
        pandas_history.reset_index(drop=False, inplace=True)
        spark_history = sqlContext.createDataFrame(pandas_history)
        
        #name of timestamp column depends on interval
        if(interval in ["1d","5d","1wk","1mo","3mo","1h"]):
            timestamp = "Date"
        else:
            timestamp = "Datetime"
            
        #transform time, add symbol, add unique id    
        spark_history = spark_history.select("Open","High","Low","Close","Volume",get_datetime_yahoo(timestamp).cast("String").alias("date"))
        spark_history = spark_history.withColumn("symbol", lit(symbol))
        spark_history = spark_history.withColumn('id',concat(col("date"),col("symbol")))
        
        #write into elasticsearch
        spark_history.write\
                    .format("es")\
                    .mode("append")\
                    .option("es.resource", interval+"/history")\
                    .option("es.mapping.id", "id")\
                    .option("es.nodes", "127.0.0.1:9200") \
                    .save()
                    
    def from_es(self,symbol,interval,spark):
        """
        Reads historical data from elasticsearch

        Parameters
        ----------
        symbol : Stock ticker symbol from US market
        interval : 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        spark : Spark Session
        
        Returns
        -------
        Dataframe containing all historical data with specified interval
        ordered by time
        
        """
        df = spark.read.format("es").load(interval+"/history")
        return df.drop("id").filter(df.symbol == symbol).orderBy("date")