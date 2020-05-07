from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import yfinance as yf
import datetime
from pytz import timezone
from pyspark.sql.window import Window
from elasticsearch import Elasticsearch

class kafka_spark_stream:
    
    """
    Spark structured streams that read from kafka_iex topics
    """
    
    def __init__(self, bootstrap):
        """
        Parameters
        ----------
        bootstrap : "127.0.0.1:9092" (local) //"10.0.0.8:9092" (BACC)

        Returns
        -------
        None.

        """
        self.bootstrap = bootstrap
    
    def stream_quotes(self, spark):
        """
        Streams data from topic iex_json_quotes.

        Returns
        -------
        parsedDF : Dataframe with 2 columns, key (symbol) and quote_data. 
        Quote_data consists of array that can be exploded.

        """
        topic = "iex_json_quotes" 
        
        streamingDF = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()

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
                .add("isUSMarketOpen", BooleanType())
        
        parsedDF = streamingDF\
                .select(col("key").cast("String"), from_json(col("value").cast("string"), schema).alias("quote_data"))
                
        return parsedDF
    
    def stream_news(self,spark):
        """
        Same as stream_quotes but for news data.
        """
        topic = "iex_json_news"
            
        streamingDF = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        parsedDF = streamingDF.select(col("key").cast("String"), col("value").cast("string"))
        
        # this step was necessary because each row contains 10 news
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
                
        return parsedDF

    def stream_company(self,spark):        
        topic = "iex_json_company" 
        
        streamingDF = spark \
                    .readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.bootstrap) \
                    .option("subscribe", topic) \
                    .option("startingOffsets", "latest") \
                    .load()

        schema = StructType() \
                        .add("symbol", StringType())\
                        .add("companyName",StringType())\
                        .add("exchange", StringType())\
                        .add("industry", StringType())\
                        .add("website", StringType())\
                        .add("description", StringType())\
                        .add("CEO",StringType())\
                        .add("securityName", StringType())\
                        .add("issueType", StringType())\
                        .add("sector", StringType())\
                        .add("employees",LongType())\
                        .add("tags", StringType())\
                        .add("address", StringType())\
                        .add("state", StringType())\
                        .add("city", StringType())\
                        .add("zip", StringType())\
                        .add("country", StringType())\
                        .add("phone", StringType())\
                        .add("primarySisCode", StringType())
        
        parsedDF = streamingDF\
            .select(col("key").cast("String"), from_json(col("value").cast("string"), schema).alias("company_data"))
            
        return parsedDF
    
    def write_console(self,df):
        writeDF = df \
            .writeStream \
            .queryName("write_console") \
            .outputMode("append") \
            .format("console") \
            .trigger(processingTime = "1 seconds")\
            .option('truncate', 'false')\
            .start()            
        return writeDF

    def write_hdfs(self,df,hdfs_path,output_dir,partition):
        """
        Parameters
        ----------
        df : Dataframe
        hdfs_path : to get hadoop's default FS -> hdfs getconf -confkey fs.defaultFS 
        on local machine it should be "hdfs://0.0.0.0:19000"
        output_dir: ? iex/quotes/date ?

        Returns
        -------
        writeDF : Stream that's being saved in hdfs

        """
        writeDF = df \
            .writeStream \
            .queryName("write_hdfs") \
            .trigger(processingTime='1 seconds') \
            .outputMode("append") \
            .format("parquet") \
            .option("checkpointLocation",hdfs_path +"/checkpoint"+"/"+output_dir) \
            .option("path", hdfs_path +"/"+ output_dir) \
            .partitionBy(partition) \
            .start()
        return writeDF
            
    def write_es(self,df,es_id,es_index):
        """
        Note: elasticsearch only supports append mode
        
        Parameters
        ----------
        df : Dataframe that will be written to elastic search
        es_id : If you use the timestamp as id, es won't write the same stock twice
        es_index : In future versions es will get rid of type, so we use the index
        also as type.
        Returns
        -------
        writeDF : Writes all rows from df as individual entries with es_id into es_index.

        """
        # random checkpoint because it didn't work when using the same checkpoint
        # path for 2 different streams
        checkpoint = str(np.random.randint(1,100000000))        
        writeDF = df \
            .writeStream\
            .outputMode("append")\
            .format("org.elasticsearch.spark.sql")\
            .option("checkpointLocation","checkpoint/" + checkpoint)\
            .option("es.resource", es_index+"/"+es_index) \
            .option("es.mapping.id", es_id)\
            .option("es.nodes", "127.0.0.1:9200") \
            .start()
        return writeDF
          
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
        Dataframe containing all historical data for given symbol with specified 
        interval ordered by time
        
        """
        df = spark.read.format("es").load(interval+"/history")
        return df.drop("id").filter(df.symbol == symbol).orderBy("date")
    
class backtest:
    
    def SimulateTrading(self, moneyOwned, stocksOwned, stockPrice, position, commission, trades):
        """
        Simple trading simulator

        Parameters
        ----------
        moneyOwned : Money which is not invested yet
        stocksOwned : Number of stocks owned
        stockPrice : Price to buy a certain stock
        position : either 1=sell, 0=hold, buy=-1
        commission : Percentage of broker commission
        trades : Number of trades

        Returns
        -------
        moneyOwned : Money which is not invested after trade
        stocksOwned : Number of stocks after trade
        total : total worth of deposit
        trades : Number of trades after trade

        """
        
        if (position < 0) and (moneyOwned>stockPrice*(1+commission)) :
        # buy one stock
            moneyOwned -= stockPrice*(1+commission)
            stocksOwned += 1
            trades += 1
        elif (position > 0) and (stocksOwned>0) :
        # sell one stock
            moneyOwned += stockPrice*(1-commission)
            stocksOwned -= 1
            trades += 1
            
        
        return (moneyOwned, stocksOwned, trades)
    
    def momentum_position(self, symbol, interval, momentum, spark):
        """
        Calculates positions for momentum strategy

        Parameters
        ----------
        symbol : stock ticker
        interval : interval of trading
        momentum : same unit as interval
        spark : spark session

        Returns
        -------
        Dataframe with date, closing price and position

        """
        data_momentum = history().from_es(symbol,interval,spark)
            
        momentum_shift = data_momentum.select("Close","date")\
                .withColumn("Close_shift", lag(data_momentum.Close)\
                .over(Window.partitionBy().orderBy("date")))
                    
        momentum_returns = momentum_shift\
                .withColumn("returns",log(momentum_shift.Close/momentum_shift.Close_shift))
                
        momentum_df = momentum_returns.\
                withColumn("position",signum(avg("returns")\
                .over(Window.partitionBy().rowsBetween(-momentum,0))))
        close_new = 'close'+symbol
        position_new = 'position'+symbol
        
        return momentum_df.select("date",momentum_df['Close'].alias(close_new),momentum_df['position'].alias(position_new)).na.drop()

    def momentum(self, symbol, interval, momentum, spark, moneyOwned, commission):
        """
        Calculates position for momentum strategy, backtests it and writes results to elasticsearch

        Parameters
        ----------
        symbol : company that's being traded, can be list
        interval : interval of stock prices to backtest
        momentum : same unit as interval
        spark : Spark Session
        moneyOwned : start capital
        commission : percentage of commission broker takes

        Returns
        -------
        None.

        """
        
        df = self.momentum_position(symbol[0], interval, momentum, spark)
        symbol_name = symbol[0]
        stocksOwned = [0]
        
        # when given list of several stocks
        if len(symbol)>1:
            for i in range(1,len(symbol)):
                symbol_name = symbol_name + symbol[i]
                stocksOwned.append(0)
                df0 = self.momentum_position(symbol[i], interval, momentum, spark)
                df = df.join(df0,"date",how='left')
        
        trades = 0
        total = moneyOwned
        es=Elasticsearch([{'host':'localhost','port':9200}])
        
        for row in df.collect():
            doc = {}
            total_init = total
            moneyInStocks = 0
            for i in range(len(symbol)):
                close = 'close'+symbol[i]
                position = 'position'+symbol[i]
                stocks = 'stocksOwned'+symbol[i]
                
                result = self.SimulateTrading(moneyOwned, stocksOwned[i], row[close], row[position], commission, trades)
                moneyOwned = result[0]
                stocksOwned[i] = result[1]
                moneyInStocks += stocksOwned[i]*row[close]*(1-commission)
                trades = result[2]
                doc[close] = row[close]
                doc[stocks] = stocksOwned[i]
            
            total = moneyOwned + moneyInStocks
            doc['date'] = row.date
            doc['symbol'] = symbol_name
            doc['moneyOwned'] = moneyOwned
            doc['total'] = total
            doc['momentum'] = momentum
            doc['trades'] = trades
            doc['returns'] = total/total_init
            es.index(index="momentum"+interval, body=doc)
    
    
    
    
    
    
    
    
    
    
    