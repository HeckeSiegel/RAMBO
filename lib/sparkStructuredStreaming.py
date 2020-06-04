from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta, date
from pytz import timezone
from pyspark.sql.window import Window
from elasticsearch import Elasticsearch
from yahoofinancials import YahooFinancials
import math as m
from alpha_vantage.sectorperformance import SectorPerformances
import operator
import pandas as pd
from ast import literal_eval
from itertools import combinations
import subprocess
import time
from scipy import stats as s

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
            .option("startingOffsets", "latest") \
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
                .select(F.col("key").cast("String"), F.from_json(F.col("value").cast("string"), schema).alias("quote_data"))
                
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
            .option("startingOffsets", "latest") \
            .load()
        
        parsedDF = streamingDF.select(F.col("key").cast("String"), F.col("value").cast("string"))
        
        # this step was necessary because each row contains 10 news
        parsedDF = parsedDF.select(parsedDF.key,\
                                       F.explode(F.array(F.get_json_object(parsedDF.value, '$[0]'),\
                                                     F.get_json_object(parsedDF.value, '$[1]'),\
                                                     F.get_json_object(parsedDF.value, '$[2]'),\
                                                     F.get_json_object(parsedDF.value, '$[3]'),\
                                                     F.get_json_object(parsedDF.value, '$[4]'),\
                                                     F.get_json_object(parsedDF.value, '$[5]'),\
                                                     F.get_json_object(parsedDF.value, '$[6]'),\
                                                     F.get_json_object(parsedDF.value, '$[7]'),\
                                                     F.get_json_object(parsedDF.value, '$[8]'),\
                                                     F.get_json_object(parsedDF.value, '$[9]'))))

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
                .select(parsedDF.key, F.from_json(parsedDF.col, schema).alias("news_data"))
                
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
            .select(F.col("key").cast("String"), F.from_json(F.col("value").cast("string"), schema).alias("company_data"))
            
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

    def write_hdfs(self,df,hdfs_path,output_dir):
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
        checkpoint = str(np.random.randint(1,100000000))
        writeDF = df \
            .writeStream \
            .queryName(output_dir) \
            .trigger(processingTime='1 seconds') \
            .outputMode("append") \
            .format("parquet") \
            .option("checkpointLocation",hdfs_path + "/"+ output_dir +"/checkpoint"+"/"+checkpoint) \
            .option("path", hdfs_path +"/"+ output_dir) \
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
        spark_history = spark_history.withColumn("symbol", F.lit(symbol))
        spark_history = spark_history.withColumn('id',F.concat(F.col("date"),F.col("symbol")))
        
        #write into elasticsearch
        spark_history.write\
                    .format("es")\
                    .mode("append")\
                    .option("es.resource", interval+"/history")\
                    .option("es.mapping.id", "id")\
                    .option("es.nodes", "127.0.0.1:9200") \
                    .save()
    
    def to_hdfs(self, symbol, interval, period, sqlContext, hdfs_path):
        """
        Writes historical stock data from yahoo finance into hdfs

        Parameters
        ----------
        symbol : stock ticker
        interval : 1m,2m,5m,15m,30m,60m,90m,1d,5d,1wk,1mo,3mo
        period : 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        sqlContext : sqlContext from SparkSession.

        Returns
        -------
        None.

        """
        if(interval in ["1d","5d","1wk","1mo","3mo","1h"]):
            timestamp = "Date"
        else:
            timestamp = "Datetime"
            
        ticker = yf.Ticker(symbol)
        pandas_history = ticker.history(period=period, interval=interval)
        pandas_history.reset_index(drop=False, inplace=True)
        spark_history = sqlContext.createDataFrame(pandas_history)
        spark_history.select(timestamp,"Open","High","Close","Volume","Low")\
                    .write.save(hdfs_path+"/historical/"+interval+"/"+symbol, format='parquet', mode='overwrite')
                    
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
    
    def from_hdfs(self, symbol, interval, sqlContext, hdfs_path):
        """
        Reads historical dara from hdfs

        Parameters
        ----------
        symbol : stock ticker
        interval : 1m,2m,5m,15m,30m,60m,90m,1d,5d,1wk,1mo,3mo
        sqlContext : sql context from spark session
        hdfs_path : local -> hdfs://0.0.0.0:19000

        Returns
        -------
        Dataframe with historical OHCL,Volume data

        """
        return sqlContext.read.format('parquet').load(hdfs_path+"/historical/"+interval+"/"+symbol) 
    
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
        commission : costs per trade in $
        trades : Number of trades

        Returns
        -------
        moneyOwned : Money which is not invested after trade
        stocksOwned : Number of stocks after trade
        total : total worth of deposit
        trades : Number of trades after trade

        """
        
        if (position > 0) and (moneyOwned>(stockPrice+commission)) :
        # buy as many stocks as possible stock
            stocksBuy = int(moneyOwned / stockPrice)
            moneyOwned -= stockPrice*(stocksBuy+commission)
            trades += 1
            stocksOwned += stocksBuy
            
        if (position < 0) and (stocksOwned>0) :
        # sell all stocks
            stocksSell = stocksOwned
            moneyOwned += stocksSell*(stockPrice-commission)
            trades += 1
            stocksOwned = 0
            
        return (moneyOwned, stocksOwned, trades)
    
    def calculate_mean_reverse(self, symbol, interval, rsi_interval, rsi_buy, rsi_sell, ibr_buy, ibr_sell, sqlContext, hdfs_path):        
        """        
        Calculates positions for mean reversion strategy  
    
        Parameters        
        ----------        
        symbol : stock ticker        
        interval : interval of trading 
        rsi_interval : interval for calculating the RSI (Relative Strength Index) ----> ex: 3 
        rsi_buy : if the RSI is under this level, this is rule to buy ----> ex: 15
        rsi_sell : if the RSI is above this level, this is rule to sell ----> ex: 60
        ibr_buy : if the IBR is under this level, this is rule to buy ----> ex: 0.2
        ibr_sell : if the IBR is above this level, this is rule to sell ----> ex: 0.7
        spark : spark session      
    
        Returns        
        -------      
        Dataframe with date, closing price and position     
        """
    
        data = history().from_hdfs(symbol, interval, sqlContext, hdfs_path) 
        #Calculating RSI (Relative Strength Index)
        df = data.select("Close","Date","Low","High").withColumn("Close_prev", F.lag(data.Close).over(Window.partitionBy().orderBy("Date")))
        df = df.withColumn("Change",df.Close-df.Close_prev)
        df = df.withColumn("Ups", F.when(F.col("Change") > 0, F.col("Change")).otherwise(0))
        df = df.withColumn("Downs", F.when(F.col("Change") < 0, -F.col("Change")).otherwise(0))
        df = df.withColumn("Higher", F.when(F.col("Change") > 0, 1).otherwise(0))
        df = df.withColumn("Lower", F.when(F.col("Change") < 0, 1).otherwise(0))
        df = df.withColumn("Higher_count",F.sum("Higher").over(Window.partitionBy().rowsBetween(-rsi_interval,0))) 
        df = df.withColumn("Lower_count",F.sum("Lower").over(Window.partitionBy().rowsBetween(-rsi_interval,0)))
        df = df.na.drop()
        df = df.withColumn("AvgU", F.avg("Ups").over(Window.partitionBy().rowsBetween(-rsi_interval,0)))
        df = df.withColumn("AvgD", F.avg("Downs").over(Window.partitionBy().rowsBetween(-rsi_interval,0)))
        df = df.withColumn("RS",df.AvgU/df.AvgD)
        df = df.withColumn("RSI",100 - 100/(1+df.RS))
        #Calculating MA (Moving Average) over 200 days
        df = df.withColumn("MA200", F.avg("Close_prev").over(Window.partitionBy().rowsBetween(-200,0)))
        #Calculating IBR (Internal Bar Range)
        df = df.withColumn("IBR", (df.Close - df.Low)/(df.High - df.Low))

        #Buy conditions
        condRSIbuy = F.col("RSI") < rsi_buy
        condMAbuy = F.col("Close") > F.col("MA200")
        condIBRbuy = F.col("IBR") < ibr_buy

        #Sell conditions
        condRSIsell = F.col("RSI") > rsi_sell
        condIBRsell = F.col("IBR") > ibr_sell

        df = df.withColumn("Position", F.when((condRSIbuy & condMAbuy & condIBRbuy), 1).otherwise(F.when((condRSIsell & condIBRsell), -1).otherwise(0)))


        close_new = 'Close' + symbol
        position_new = 'Position' + symbol
        date_new = 'Datetime'
        df = df.select(df["Date"].alias(date_new), df["Close"].alias(close_new),df["Position"].alias(position_new))
    
        df_final = df.na.drop()
    
        return df_final  
    
    def buy_and_hold_portfolio_position(self, df, symbol, interval, sqlContext):
        """
        Strategy that just buys as many stocks as possible (until no more money) and holds
        onto them.

        Parameters
        ----------
        symbol : stock ticker
        interval : granularity of stock prices
        sqlContext : from spark session
        hdfs_path : local -> "hdfs://0.0.0.0:19000"

        Returns
        -------
        Dataframe with time, price and position -1 (=buy) for all prices

        """
        
        if (interval in ["1d","5d","1wk","1mo","3mo"]):
            timestamp = "Date"
        else:
            timestamp = "Datetime"
        
        spark_df = df.na.drop().select("*").orderBy(timestamp)
        
        for symbol in symbol:
            position = "Position"+symbol
            close = "Close"+symbol
            spark_df = spark_df.withColumn(position, F.lit(1))\
                .withColumnRenamed(symbol, close)
        
        return spark_df.na.drop()
    
    def momentum_portfolio_position(self, df, symbol, interval, momentum, sqlContext):
        
        spark_df = df.na.drop().select("*")
        
        for symbol in symbol:
            spark_df = spark_df.select("*")\
                        .withColumn(symbol+"_shift", F.lag(F.col(symbol))\
                        .over(Window.partitionBy().orderBy("Datetime")))
    
            spark_df =  spark_df\
                        .withColumn("returns"+symbol,F.log(F.col(symbol)/F.col(symbol+"_shift")))
    
            spark_df = spark_df.\
                        withColumn("Position"+symbol,F.signum(F.avg("returns"+symbol)\
                        .over(Window.partitionBy().rowsBetween(-momentum,Window.currentRow))))
    
            spark_df = spark_df.drop(symbol+"_shift","returns"+symbol).withColumnRenamed(symbol,"Close"+symbol)
        
        return spark_df.na.drop()
    
    def mean_reverse_portfolio_position(self, symbol, interval, rsi_interval, rsi_buy, rsi_sell, ibr_buy, ibr_sell, sqlContext, hdfs_path):        
        """        
        Getting the positions for the mean reverse strategy of a whole portfolio        
        
        Parameters        
        ----------        
        symbol : stock ticker 
        interval : granularity of stock prices 
        rsi_interval : interval for calculating the RSI (Relative Strength Index) ----> ex: 3 
        rsi_buy : if the RSI is under this level, this is rule to buy ----> ex: 15
        rsi_sell : if the RSI is above this level, this is rule to sell ----> ex: 60
        ibr_buy : if the IBR is under this level, this is rule to buy ----> ex: 0.2
        ibr_sell : if the IBR is above this level, this is rule to sell ----> ex: 0.7
            
        sqlContext : from spark session       
        hdfs_path : local -> "hdfs://0.0.0.0:9000"
    
        Returns        
        -------        
        df : dataframe with all closing prices and positions     
        """      
        # dataframe for positions of momentum strategy      
        df = self.calculate_mean_reverse(symbol[0], interval, rsi_interval, rsi_buy, rsi_sell, ibr_buy, ibr_sell, sqlContext, hdfs_path)  
           
        for i in range(1,len(symbol)): 
            if (symbol[i] != "^IXIC"):
                df0 = self.calculate_mean_reverse(symbol[i], interval, rsi_interval, rsi_buy, rsi_sell, ibr_buy, ibr_sell, sqlContext, hdfs_path)         
                df = df.join(df0,"Datetime",how='left')   
    
        return df.na.drop()
    
    
    def depot(self, depotId, symbol, share, position, startCap, startCap_market,commission, risk_free, performance_market, beta_stocks, trades, pr):
        """
        Calculates performance for given positions

        Parameters
        ----------
        position: dataframe which contains dates, closing prices and positions for all symbols + nasdaq
        depotId: Id to identify different depots
        symbol : company that's being traded, can be list
        share: distribution of startCap, has to be normalized and len(share)=len(symbol)+1
        momentum : same unit as interval
        startCap: start capital
        commission : percentage of commission broker takes
        risk_free : monthly risk free market return
        
        Returns
        -------
        value : Wert in $
        startCap : Start-Capital in $
        profit : Profit in $
        start_date : Beginning of backtest
        trades_total : total trades
        Performance : performance in %
        beta_avg : beta of strategy
        alpha : alpha of strategy

        """

        # number of stocks owned for each company
        stocksOwned = [0]
        # total number of trades
        trades_total = trades
        # current value of your depot
        value = startCap
        # to get the start date of historical data
        start_date_count = 0
        # distribution of cash which can be invested
        moneyForInvesting =[startCap*share[0]]
        # dictionary with betas for all symbols
        beta_list = []
        # money in stocks for indiviudal companys
        moneyInStocks_list = [0]
        
        # when given list of several stocks
        if len(symbol)>1:
            for i in range(1,len(symbol)):
                stocksOwned.append(0)
                moneyInStocks_list.append(0)
                moneyForInvesting.append(startCap*share[i])
        
        df = position
        for row in df.collect():
            
            # get start date of historical data and beginning price of nasdaq index
            if start_date_count == 0:
                start_date = row.Datetime
                start_date_count += 1
                
            # current value before trading
            value_init = value
            
            # set to 0 before every trading cycle to calculate new
            moneyInStocks = 0
            
            # money which will not be invested
            moneyFree = 0
            
            # calculate beta for each moneyInStocks distribution new
            beta_current = 0
            
            # go through all companies
            for i in range(len(symbol)):
                close = "Close"+symbol[i]
                position = "Position"+symbol[i]
                result = self.SimulateTrading(moneyForInvesting[i], stocksOwned[i], row[close], row[position], commission, trades_total)
                moneyForInvesting[i] = result[0]
                stocksOwned[i] = result[1]
                trades_total = result[2]
                # money currently invested in stocks
                moneyInStocks_list[i] = stocksOwned[i]*(row[close]-commission)
                moneyInStocks += moneyInStocks_list[i]
                # free money that can be used to buy stocks
                moneyFree += moneyForInvesting[i]
                try:
                    beta_current += beta_stocks[symbol[i]]*moneyInStocks_list[i]/value
                except:
                    beta_current = None
                
            # total current value of depot (free money + invested money)
            value = moneyInStocks + moneyFree
            moneyForInvesting = [share[i]*moneyFree for i in range(len(symbol))]
            beta_list.append(beta_current)
            end_date = row.Datetime
            
        # number of days this strategy was tested
        time = end_date - start_date
        days = m.ceil(time.total_seconds()/(60*60*24))
        rf = days * (m.pow(risk_free+1,1/30) - 1) * 100
        # profit of strategy in $
        profit = value - startCap
        # performance in %
        performance_depot = (value/startCap - 1)*100
        # average beta
        try:
            beta_avg = round(sum(beta_list)/len(beta_list),2)
            # alpha of portfolio
            alpha = round(performance_depot - rf - beta_avg*(performance_market - rf),2)
        except:
            beta_avg = None
            alpha = None
            
        value_market = startCap_market*(1 + performance_market/100)
        
        return (depotId,round(value,2),alpha,beta_avg, startCap, round(profit,2), start_date, end_date, trades_total, round(performance_depot,2), performance_market, round(value_market,2), round(startCap_market,2))
    
    def market_performance(self, interval, d_start, d_end ,sqlContext, hdfs_path):
        """
        Performance of S&P500 for given interval

        Parameters
        ----------
        interval : granularity of quotes
        sqlContext : from soark session
        hdfs_path : "hdfs://0.0.0.0:19000"
        d_start : start date
        d_end : end date
        Returns
        -------
        market performance in %

        """
        if(interval in ["1d","5d","1wk","1mo","3mo","1h"]):
            timestamp = "Date"
        else:
            timestamp = "Datetime"
        
        historical_pd = yf.download("^GSPC",start=d_start,end=d_end,interval=interval)
        historical_pd.reset_index(drop=False, inplace=True)
        historical_pd.drop(historical_pd.tail(1).index,inplace=True)
        snp500 = sqlContext.createDataFrame(historical_pd)
        
        snp500acs = snp500.orderBy(timestamp)
        first = snp500acs.first()
        snp500desc = snp500.orderBy(F.col(timestamp).desc())
        last = snp500desc.first()
        performance = round((last.Close/first.Close - 1)*100,2)
        return (performance)
    
    def depotId_func(self, sqlContext, hdfs_path, directory):
        """
        Finds out latest depotid

        """
        try:
            df = sqlContext.read.format('parquet').load(hdfs_path+"/"+directory).orderBy(F.col("DepotId").desc())
            depotId = df.first().DepotId + 1
        except:
            depotId = 1
            
        return depotId
    
    def max_sector_symbols(self, period, n_sector, n_stock, distribution):
        """
        Find symbols for momentum strategy

        Parameters
        ----------
        period : "1d" or "5d"

        Returns
        -------
        symbols_max : list with symbols
        share : normalized list of shares

        """
        sp = SectorPerformances(key='JLD6KKU8CQTZD02V',output_format='json')
        data, meta_data = sp.get_sector()
        
        if(period == "1d"):
            key = 'Rank A: Real-Time Performance'
        if(period == "5d"):
            key = 'Rank C: 5 Day Performance'
        
        performance = data[key]
        sector = [k for k,v in performance.items()][:n_sector]
        
        for i in range(n_sector):
            if(sector[i] == 'Communication Services'):
                sector[i] = 'Telecommunication Services'
            
        df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
    
        symbol = df[df['Sector'].isin(sector)].index.tolist()
        
        pandas_history = yf.download(symbol,period=period,interval=period)
        
        returns = pandas_history['Close'].head(1).div(pandas_history['Open'].head(1)).T
        returns.rename(columns={returns.columns[0]:"returns"}, inplace=True)
        returns.index.rename('Symbol', inplace=True)
        
        symbols_max = (returns.sort_values('returns',ascending=False)).index.tolist()[:n_stock]
        
        if(distribution == "equal"):
            share = [1/len(symbols_max) for i in range(len(symbols_max)-1)]
            share.append(1 - sum(share))
        
        if(distribution == "desc"):
            share = [i for i in range(n_stock,0,-1)]
            share = [i/sum(share) for i in share]
            
        return symbols_max, share, sector
    
    def min_pe_symbols(self, symbol):
        """
        Looks through all S&P500 comanies or through given list of stocks and finds the one
        with highest performance and lowest pe ratio

        Parameters
        ----------
        symbol : list of symbols has to be at least 2 companies or None
        n_stock : number of stocks function gives out
        distribution : distribution of shares, "equal" or "desc"
        Returns
        -------
        list with symbols and corresponding shares
        
        """
        # read in all S&P500 companies if no symbols given
        if(symbol == None):
            df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
            symbol = df.index.tolist()
            return symbol
        
        # get start and end date
        d = datetime.today() - timedelta(days=2)
        weekday = d.strftime("%A")
    
        if(weekday == 'Saturday'):
            d = d - timedelta(days=1)
        if(weekday == 'Sunday'):
            d = d - timedelta(days=2)
    
        d_start = d.strftime("%Y-%m-%d")
        d_end = (d + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # read in historical data from S&P500 companies
        pandas_history = yf.download(symbol,start=d_start,end=d_end)
        
        # calculate newest performance and sort in descending order
        returns = pandas_history['Close'].tail(1).div(pandas_history['Open'].tail(1)).T
        returns.rename(columns={returns.columns[0]:"returns"}, inplace=True)
        returns.index.rename('Symbol', inplace=True)
        returns_list = (returns.sort_values('returns',ascending=False)).index.tolist()
        
        # calculate pe_ratios
        close = pandas_history['Close'].tail(1).T
        close.rename(columns={close.columns[0]:"pe_ratio"}, inplace=True)
        close.index.rename('Symbol',inplace=True)
        for i,row in close.iterrows():
            eps = YahooFinancials(i).get_earnings_per_share()
            if(eps == None):
                close.drop(i, inplace=True)
                continue
            close.loc[i,'pe_ratio'] = row.pe_ratio/eps
    
        pe_list = (close.sort_values('pe_ratio',ascending=True)).index.tolist()
    
        # rank them according to performance and pe ratio
        doc = {}
        for j in range(len(returns_list)):
            doc[returns_list[j]] = j
        for i in range(len(pe_list)):
            doc[pe_list[i]] = i
            
        symbol = [k for k, v in sorted(doc.items(), key=lambda item: item[1])]
        
        with open("symbol.txt", "w") as output:
                output.write(str(symbol))
        
        return symbol
    
    def share_func(self, n):
        """
        Calculates shares for given number of stocks for 2 different distributions

        Parameters
        ----------
        n : number of stocks

        Returns
        -------
        equal : equally distributed
        desc : descending

        """
        equal = [round(float(1/n),2) for i in range(n-1)]
        equal.append(round(float(1) - sum(equal), 2))
    
        #desc = [float(i) for i in range(n,0,-1)]
        #desc = [round(float(i/sum(desc)), 2) for i in desc]
        if(sum(equal) > 1):
            equal[0] -= (sum(equal)-1)
            
        return [equal]
    
    def get_eps(self,symbol):
        """
        Get earnings per share either for given symbols or for all S&P500 companys
        and write them into txt file as dictionary.

        Parameters
        ----------
        symbol : List of symbols or None

        Returns
        -------
        None.

        """
        if(symbol == None):
            df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
            symbol = df.index.tolist()
            
        eps_dic = {}
        for symbol in symbol:
            eps_dic[symbol] = YahooFinancials(symbol).get_earnings_per_share()
            
        eps_dic = {k: v for k, v in eps_dic.items() if v is not None}
        with open("symbols/earnings_per_share.txt", "w") as output:
            output.write(str(eps_dic))
    
    def get_payout(self,symbol):
        """
        Same as get_eps but for payout ratio

        Returns
        -------
        None.

        """
        if(symbol == None):
            df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
            symbol = df.index.tolist()
    
        payout_dic = {}
        for symbol in symbol:
            payout_dic[symbol] = YahooFinancials(symbol).get_payout_ratio()
    
        payout_dic = {k: v for k, v in payout_dic.items() if v is not None}
        with open("symbols/payout.txt", "w") as output:
            output.write(str(payout_dic))
            
    def performance_sorted(self,df,start,period,symbol):
        """
        Calculates performance of symbols over given period and sorts it 

        Parameters
        ----------
        period : integer in days e.g. 1/5/30
        symbol : list of symbols or None

        Returns
        -------
        returns_list : sorted list of symbols in descending order

        """
    
        # read in historical data from S&P500 companies
        pandas_history = df.copy(deep=True)
        pandas_history = pandas_history[pandas_history.index.date<start]
        pandas_history = pandas_history['Close'].tail(1+period)
        # get oldest and most recent prices
        past = pandas_history.head(1)
        now = pandas_history.tail(1)
        # reset index to divide both dataframes
        past.reset_index(inplace=True)
        now.reset_index(inplace=True)
        past.drop(columns='Date', inplace=True)
        now.drop(columns='Date', inplace=True)
    
        # divide newest stock price by oldest
        returns = now.div(past).T
        returns.rename(columns={returns.columns[0]:"returns"}, inplace=True)
        returns.index.rename('Symbol', inplace=True)
    
        # sort in descending order
        returns_list = (returns.sort_values('returns',ascending=False)).index.tolist()
        
        return returns_list
    
    def eps_sorted(self):
        """
        Reads in earnings per share of S&P500 companys, sorts it in descending order

        Returns
        -------
        symbols_sorted : TYPE
            DESCRIPTION.

        """
        with open("symbols/earnings_per_share.txt", "r") as f:
            eps = f.read()
    
        eps = literal_eval(eps)
        symbols_sorted = [k for k, v in sorted(eps.items(), key=lambda item: item[1], reverse=True)]
        
        return symbols_sorted
    
    def pe_sorted(self,d_start):
        """
        Calculates pe-ratio for all S&P500 companys, sorts them in ascending order

        Returns
        -------
        None.

        """
        df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
        symbol = df.index.tolist()
        
        # get start and end date
        d = d_start - timedelta(days=1)
        weekday = d.strftime("%A")
        
        if(weekday == 'Saturday'):
            d = d - timedelta(days=1)
        if(weekday == 'Sunday'):
            d = d - timedelta(days=2)
        
        d_start = d.strftime("%Y-%m-%d")
        d_end = d_start
    
        # read in historical data from S&P500 companies
        pandas_history = yf.download(symbol,start=d_start,end=d_end)
        # read in earnings per share
        with open("symbols/earnings_per_share.txt", "r") as f:
            eps = f.read()
    
        eps = literal_eval(eps)
    
        # calculate pe_ratios
        close = pandas_history['Close'].tail(1).T
        close.rename(columns={close.columns[0]:"pe_ratio"}, inplace=True)
        close.index.rename('Symbol',inplace=True)
    
        for i,row in close.iterrows():
            try:
                close.loc[i,'pe_ratio'] = row.pe_ratio/eps[i]
            except:
                close.drop(i, inplace=True)
    
        pe_list = (close.sort_values('pe_ratio',ascending=True)).index.tolist()
        
        return pe_list
    
    def get_historical_daily(self, symbol, start, period):
    
        if(symbol == None):
            df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
            symbol = df.index.tolist()
            
    
        # get start and end date
        d_start = start - timedelta(days=period)
        d_end = start
    
        try:
            df = yf.download(symbol,start=d_start,end=d_end,interval="1d")
        except:
            d_start = d_start - timedelta(days=1)
            
        weekday = d_start.strftime("%A")
            
        if(weekday == 'Saturday'):
            d_start -= timedelta(days=1)
            df = yf.download(symbol,start=d_start,end=d_end,interval="1d")
        if(weekday == 'Sunday'):
            d_start -= timedelta(days=2)
            df = yf.download(symbol,start=d_start,end=d_end,interval="1d")
        
        return df
    
    def get_historical_minute(self, symbol, start, end):
        """
        Use this to get historical intraday data for backtesting. Period between start and
        end can't be more than 7 days

        Parameters
        ----------
        symbol : List of companys
        start : start date
        end : TYPE
            DESCRIPTION.

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        if(symbol == None):
            df = pd.read_csv("symbols/constituents.csv", index_col='Symbol').drop(columns='Name')
            symbol = df.index.tolist()
            
        end = end + timedelta(days=2)
    
        try:
            df = yf.download(symbol,start=start,end=end,interval="1m")
        except:
            start = start - timedelta(days=1)
            
        weekday = start.strftime("%A")
            
        if(weekday == 'Saturday'):
            start -= timedelta(days=1)
            df = yf.download(symbol,start=start,end=end,interval="1m")
        if(weekday == 'Sunday'):
            start -= timedelta(days=2)
            df = yf.download(symbol,start=start,end=end,interval="1m")
    
        
        return df['Close']
    
    def full_combination_symbols(self,d_start,period,df):
        """
        Combines indicator ranks in all possible ways

        Returns
        -------
        symbol_full_list : list of list of ranked symbols

        """
        symbol_full_list = [self.performance_sorted(df,d_start,period[0],None)]
        names = [[period[0]]]
        
        # get symbol lists for each indicator
        for p in range(1,len(period)):
            returns = self.performance_sorted(df,d_start,period[p],None)
            symbol_full_list.append(returns)
            names.append([period[p]])
        
        # all possible combinations
        n_comb = len(names) + 1
        comb = [list(combinations([i for i in range(1,n_comb)], 1))]
        for j in range(2,6):
            comb.append(list(combinations([i for i in range(1,n_comb)], j)))
        
        for i in range(1,len(comb)):
            for j in range(len(comb[i])):
                rank = {k:0 for k in symbol_full_list[0]}
                indicator = []
                for k in range(len(comb[i][j])):
                    symbol = symbol_full_list[comb[i][j][k]-1]
                    n = len(symbol)
                    indicator.append(names[comb[i][j][k]-1][0])
                    for l in range(n):
                        try:
                            rank[symbol[l]] += l
                        except:
                            continue
                symbol_sorted = [k for k, v in sorted(rank.items(), key=lambda item: item[1])]
                symbol_full_list.append(symbol_sorted)
                names.append(indicator)
        
        return symbol_full_list, names
    
    def performance_simple(self, startCap, commission, risk_free, strategy, interval, period, symbol_list, symbol_strategy,n_stock, hdfs_path, sqlContext):
        """
        Calculates performance of given strategy and performance of buy and hold strategy for the same stocks + shares
        so that one can compare if it would have been better to just buy and hold the stocks

        Parameters
        ----------
        symbol : List of stock tickers
        share : Distribution of stocks in %
        startCap : Start Capital
        commission : Transaction fee
        risk_free : Monthly risk free market return
        strategy : strategy type
        interval : granularity of stock prices
        hdfs_path : local -> "hdfs://0.0.0.0:19000"
        sqlContext : from spark session

        Returns
        -------
        performance_data : depot performance for given strategy and buy and hold strategy
        depot_data : information about strategy

        """
            
        # get market performance
        market = self.market_performance(interval, period, sqlContext, hdfs_path)
        
        # get latest depotId
        depotId = self.depotId_func(sqlContext, hdfs_path)
        
        count = -1
        
        for sym in symbol_list:
            count += 1
            
            # get market beta of individual stocks
            beta = YahooFinancials(sym[:max(n_stock)]).get_beta()
            if("^GSPC" in sym[:max(n_stock)]):
                beta["^GSPC"] = 1.0
        
            for n in n_stock:
                symbol = sym[:n]
                shares = self.share_func(len(symbol))
                # get historical data from yahoofincance
                historical_pd = yf.download(symbol,period=period,interval=interval)
                historical_pd = historical_pd['Close']
                historical_pd.reset_index(drop=False, inplace=True)
                historical_spark = sqlContext.createDataFrame(historical_pd)
       
                for share in shares:
                    
                    if(strategy[0] == "momentum"):
                        # initialize position, depotId, performance for momentum strategy
                        position_momentum = [self.momentum_portfolio_position(historical_spark, symbol, interval, period, strategy[1], sqlContext)]
                        depotId_momentum = [depotId]
                        performance_data = [self.depot(depotId_momentum[0], symbol, share, position_momentum[0], startCap, commission, risk_free, market, beta)]
                        depot_data = [(depotId_momentum[0],startCap,strategy[0]+str(strategy[1]),symbol,share,symbol_strategy[count])]
                        
                        for i in range(2,len(strategy)):
                            # loop through rest of momentum strategy to get performance for other momentums
                            pos = self.momentum_portfolio_position(historical_spark, symbol, interval, period, strategy[i], sqlContext)
                            position_momentum.append(pos)
                            depotId += 1
                            depotId_momentum.append(depotId)
                            performance_momentum = self.depot(depotId_momentum[i-1], symbol, share, position_momentum[i-1], startCap, commission, risk_free, market, beta)
                            performance_data.append(performance_momentum)
                            depot_momentum = (depotId_momentum[i-1],startCap,strategy[0]+str(strategy[i]),symbol,share,symbol_strategy[count])
                            depot_data.append(depot_momentum)
                            
                    if (strategy[0] == "mean_reverse"):
                        rsi_buy = strategy[1]
                        rsi_sell = strategy[2]
                        ibr_buy = strategy[3]
                        ibr_sell = strategy[4]
                        #calculate positions for mean reverse strategy
                        position1 = [self.mean_reverse_portfolio_position(symbol, interval, strategy[5], rsi_buy, rsi_sell, ibr_buy, ibr_sell,  sqlContext, hdfs_path)]
                        for i in range(6,len(strategy)):            
                            pos = self.mean_reverse_portfolio_position(symbol, interval, strategy[i], rsi_buy, rsi_sell, ibr_buy, ibr_sell,sqlContext, hdfs_path)    
                            position1.append(pos)
                         # calculate performance for mean reverse strategy        
                        depotId1 = [depotId]
                        performance_data = [self.depot(depotId1[0], symbol, share, position1[0], startCap, commission, risk_free, market, beta)]
                        depot_data = [(depotId1[0],startCap,strategy[0]+str(strategy[5]),symbol,share)]
                        for i in range(5,len(position1)+4):
                            depotId += 1
                            depotId1.append(depotId)
                            depot1 = self.depot(depotId1[i-4], symbol, share, position1[i-4], startCap, commission, risk_free, market, beta)
                            performance_data.append(depot1)
                            data = (depotId1[i-4],startCap,strategy[0]+str(strategy[i+1]),symbol,share)
                            depot_data.append(data)
             
                    # performance for buy and hold strategy
                    depotId += 1
                    position_bnh = self.buy_and_hold_portfolio_position(historical_spark, symbol, interval, period, sqlContext, hdfs_path)
                    performance_bnh = self.depot(depotId, symbol, share, position_bnh, startCap, commission, risk_free, market, beta)
                    performance_data.append(performance_bnh)
                    depot_bnh = (depotId,startCap,"Buy and Hold",symbol,share,symbol_strategy[count])
                    depot_data.append(depot_bnh)
                    depotId += 1
                    
                    # write performance and depot data to hdfs
                    schema_performance = StructType() \
                            .add("DepotId", LongType())\
                            .add("Value",DoubleType())\
                            .add("Alpha", DoubleType())\
                            .add("Beta", DoubleType())\
                            .add("Start-Capital", DoubleType())\
                            .add("Profit", DoubleType())\
                            .add("Start-Date",DateType())\
                            .add("End-Date",DateType())\
                            .add("Trades", LongType())\
                            .add("Performance_Strategy", DoubleType())\
                            .add("Performance_S&P500", DoubleType())
                    df_performance = sqlContext.createDataFrame(performance_data,schema_performance)
                    df_performance.write.save(hdfs_path+"/performance", format='parquet', mode='append')
                    
                    schema_depot = StructType() \
                            .add("DepotId", LongType())\
                            .add("Start-Capital",DoubleType())\
                            .add("Strategy", StringType())\
                            .add("ISIN", ArrayType(StringType()))\
                            .add("Share", ArrayType(DoubleType()))\
                            .add("Symbol_Strategy", StringType())
                            
                    df_depot = sqlContext.createDataFrame(depot_data,schema_depot)
                    df_depot.write.save(hdfs_path+"/depot", format='parquet', mode='append')
        
        return performance_data,depot_data
    
    def performance_full(self, df_daily, startCap, startCap_market, commission, risk_free, strategy, interval, d_start, symbol_list, symbol_strategy, n_stock, hdfs_path, sqlContext, hdfs_dir):
        """
        Calculates performance for several different shares, strategies, number of companys for given day

        Parameters
        ----------
        startCap : TYPE
            DESCRIPTION.
        commission : TYPE
            DESCRIPTION.
        risk_free : TYPE
            DESCRIPTION.
        strategy : TYPE
            DESCRIPTION.
        interval : TYPE
            DESCRIPTION.
        d_start : TYPE
            DESCRIPTION.
        d_end : TYPE
            DESCRIPTION.
        symbol_list : TYPE
            DESCRIPTION.
        symbol_strategy : TYPE
            DESCRIPTION.
        n_stock : TYPE
            DESCRIPTION.
        hdfs_path : TYPE
            DESCRIPTION.
        sqlContext : TYPE
            DESCRIPTION.

        Returns
        -------
        performance_data : TYPE
            DESCRIPTION.
        depot_data : TYPE
            DESCRIPTION.

        """ 
        
        # get market performance
        market = self.market_performance(interval, d_start, d_start+timedelta(days=1), sqlContext, hdfs_path)
        
        # get latest depotId
        depotId = self.depotId_func(sqlContext, hdfs_path,"backtest/performance_full"+hdfs_dir+"/"+d_start.strftime("%Y%m%d"))
        
        count = -1
        
        for sym in symbol_list:
            count += 1
            
            # get market beta of individual stocks
            try:
                beta = YahooFinancials(sym[:max(n_stock)]).get_beta()
            except:
                beta = None
                
            if("^GSPC" in sym[:max(n_stock)]):
                beta["^GSPC"] = 1.0
        
            for n in n_stock:
                symbol = sym[:n]
                shares = self.share_func(len(symbol))
                # get historical data from yahoofincance
                df_copy = df_daily.copy(deep=True)
                historical_pd = df_copy[symbol]
                historical_pd = historical_pd[historical_pd.index.date == d_start]
                historical_pd.reset_index(drop=False, inplace=True)
                historical_spark = sqlContext.createDataFrame(historical_pd)
       
                for share in shares:
                    
                    if(strategy[0] == "momentum"):
                        # initialize position, depotId, performance for momentum strategy
                        position_momentum = [self.momentum_portfolio_position(historical_spark, symbol, interval, strategy[1], sqlContext)]
                        depotId_momentum = [depotId]
                        performance_data = [self.depot(depotId_momentum[0], symbol, share, position_momentum[0], startCap, startCap_market, commission, risk_free, market, beta, 0, False)]
                        depot_data = [(depotId_momentum[0],startCap,strategy[0]+str(strategy[1]),symbol,share,symbol_strategy[count])]
                        
                        for i in range(2,len(strategy)):
                            # loop through rest of momentum strategy to get performance for other momentums
                            pos = self.momentum_portfolio_position(historical_spark, symbol, interval, strategy[i], sqlContext)
                            position_momentum.append(pos)
                            depotId += 1
                            depotId_momentum.append(depotId)
                            performance_momentum = self.depot(depotId_momentum[i-1], symbol, share, position_momentum[i-1], startCap, startCap_market, commission, risk_free, market, beta, 0, False)
                            performance_data.append(performance_momentum)
                            depot_momentum = (depotId_momentum[i-1],startCap,strategy[0]+str(strategy[i]),symbol,share,symbol_strategy[count])
                            depot_data.append(depot_momentum)
             
                    # performance for buy and hold strategy
                    depotId += 1
                    position_bnh = self.buy_and_hold_portfolio_position(historical_spark, symbol, interval, sqlContext)
                    performance_bnh = self.depot(depotId, symbol, share, position_bnh, startCap, startCap_market, commission, risk_free, market, beta, 0, False)
                    performance_data.append(performance_bnh)
                    depot_bnh = (depotId,startCap,"Buy and Hold",symbol,share,symbol_strategy[count])
                    depot_data.append(depot_bnh)
                    depotId += 1
                    
                    # write performance and depot data to hdfs
                    schema_performance = StructType() \
                            .add("DepotId", LongType())\
                            .add("Value",DoubleType())\
                            .add("Alpha", DoubleType())\
                            .add("Beta", DoubleType())\
                            .add("Start-Capital", DoubleType())\
                            .add("Profit", DoubleType())\
                            .add("Start-Date",DateType())\
                            .add("End-Date",DateType())\
                            .add("Trades", LongType())\
                            .add("Performance_Strategy", DoubleType())\
                            .add("Performance_S&P500", DoubleType())\
                            .add("Value_S&P500", DoubleType())\
                            .add("Start-Capital_S&P500", DoubleType())
                            
                    df_performance = sqlContext.createDataFrame(performance_data,schema_performance)
                    df_performance.write.save(hdfs_path+"/backtest/performance_full"+hdfs_dir+"/"+d_start.strftime("%Y%m%d"), format='parquet', mode='append')
                    
                    schema_depot = StructType() \
                            .add("DepotId", LongType())\
                            .add("Start-Capital",DoubleType())\
                            .add("Strategy", StringType())\
                            .add("ISIN", ArrayType(StringType()))\
                            .add("Share", ArrayType(DoubleType()))\
                            .add("Symbol_Strategy", ArrayType(LongType()))
                            
                    df_depot = sqlContext.createDataFrame(depot_data,schema_depot)
                    df_depot.write.save(hdfs_path+"/backtest/depot_full"+hdfs_dir+"/"+d_start.strftime("%Y%m%d"), format='parquet', mode='append')
        
        return performance_data,depot_data
    
    def new_borders(self,list_old,most_frequent):
        
        list_new = [0,0,0]
        list_new[1] = most_frequent
        list_new[0] = list_old[0] + int((most_frequent-list_old[0])/2)
        if(list_new[0] < 1):
            list_new[0] = 1
        list_new[2] = most_frequent + m.ceil((list_old[2]-most_frequent)/2)
        if(list_new[2] == most_frequent):
            list_new[2] += 1
        
        return list_new
    
    def new_parameters(self,DepotId, df_depot, period, strategy, n_stock):
        
        period_list = []
        strategy_list = []
        n_stock_list = []
    
        for Id in DepotId:
            row = df_depot.filter(F.col("DepotId")==Id).head()
            for i in row["Symbol_Strategy"]:
                period_list.append(i)
            strategy_list.append(row.Strategy)
            n_stock_list.append(len(row.ISIN))
        
        period_mf = int(s.mode(period_list)[0])
        strategy_mf = str(s.mode(strategy_list)[0])[2:-2]
        n_stock_mf = int(s.mode(n_stock_list)[0])
    
        period_new = self.new_borders(period,period_mf)
        n_stock_new = self.new_borders(n_stock,n_stock_mf)
        
        if(strategy_mf == 'Buy and Hold'):
            strategy_new = ['momentum']
            for i in strategy[1:]:
                strategy_new.append(i)
        else:
            strategy_new = []
            strategy_new.append("momentum")
            res = self.new_borders(strategy[1:],int(strategy_mf[8:]))
            for i in res:
                strategy_new.append(i)
        
        return period_new,n_stock_new,strategy_new
    
    def performance_loop(self,df_daily, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext, hdfs_dir, best_of):
        """
        Loop through time period and do full performance for each day

        Parameters
        ----------
        startCap : Start Capital
        commission : trading fee
        risk_free : risk free market return
        strategy : Trading Strategy
        interval : Granularity of stock quotes
        start : start date of backtesting
        end : end date of backtesting
        n_stock : Number of stocks in portfolio
        hdfs_path : ...
        sqlContext : ...

        Returns
        -------
        None.

        """
        day = start
        while(day <= end):
            weekday = day.strftime("%A")
    
            if(weekday == 'Saturday'):
                day += timedelta(days=2)
            if(weekday == 'Sunday'):
                day += timedelta(days=1)
            if(day==date(2020,5,25)):
                day += timedelta(days=1)
            
            if(hdfs_dir == "_ml"):
                day_prior = day - timedelta(days=1)
                if(day_prior == date(2020,5,25)):
                    day_prior = date(2020,5,22)
                        
                # make sure that it's not trying to stream quote data from weekend
                weekday = day_prior.strftime("%A")
        
                if(weekday == 'Saturday'):
                    day_prior -= timedelta(days=1)
                if(weekday == 'Sunday'):
                    day_prior -= timedelta(days=2)
                try:
                    old = sqlContext.read.format('parquet').load("hdfs://0.0.0.0:19000/backtest/ml").orderBy(F.col("Date").desc()).head()
                    
                    period_last = old["Period"]
                    strategy_last = [old["Strategy-Name"]]
                    for i in old["Strategy"]:
                        strategy_last.append(i)
                    n_stock_last = old["Stock"]
                    df_performance = sqlContext.read.format('parquet').load("hdfs://0.0.0.0:19000/backtest/performance_full_ml/"+day_prior.strftime("%Y%m%d")).orderBy(F.col("Performance_Strategy").desc())
                    df_depot = sqlContext.read.format('parquet').load("hdfs://0.0.0.0:19000/backtest/depot_full_ml/"+day_prior.strftime("%Y%m%d"))
                    DepotId = [row.DepotId for row in df_performance.head(best_of)]
                    
                    result = self.new_parameters(DepotId, df_depot, period_last, strategy_last, n_stock_last)
                    period = result[0]
                    n_stock = result[1]
                    strategy = result[2]
                
                except Exception as e:
                    print("fail",e)
                    period = period
                    n_stock = n_stock
                    strategy = strategy
                    
                schema_ml = StructType() \
                                .add("Date", DateType())\
                                .add("Period",ArrayType(LongType()))\
                                .add("Strategy-Name", StringType())\
                                .add("Strategy", ArrayType(LongType()))\
                                .add("Stock", ArrayType(LongType()))
                                
                data_ml = [(day,period,strategy[0],strategy[1:],n_stock)]
                df_ml = sqlContext.createDataFrame(data_ml,schema_ml)
                df_ml.write.save(hdfs_path+"/backtest/ml", format='parquet', mode='append')
                    
                symbol_list , symbol_strategy = self.full_combination_symbols(day, period, df_daily)
                self.performance_full(df_minute, startCap, startCap_market, commission, risk_free, strategy, interval, day, symbol_list, symbol_strategy, n_stock, hdfs_path, sqlContext, hdfs_dir)
            else:
                symbol_list , symbol_strategy = self.full_combination_symbols(day, period, df_daily)
                self.performance_full(df_minute, startCap, startCap_market, commission, risk_free, strategy, interval, day, symbol_list, symbol_strategy, n_stock, hdfs_path, sqlContext, hdfs_dir)
                
            day += timedelta(days=1)
    
    
    def get_symbol(self, df, start_best, symbol_strategy, n_symbol):
        """
        Get n_symbol symbols for given strategy

        Parameters
        ----------
        symbol_strategy : Strategy to choose symbols
        n_symbol : Number of symbols

        Returns
        -------
        symbol_sorted : Returns list of length n_symbols with stock tickers

        """
        # get symbol lists for each indicator
        symbol_full_list = [self.performance_sorted(df,start_best,symbol_strategy[0],None)]
        for p in range(1,len(symbol_strategy)):
            returns = self.performance_sorted(df,start_best,symbol_strategy[p],None)
            symbol_full_list.append(returns)
        
        # ranke them according each strategy and add rankings
        rank = {k:0 for k in symbol_full_list[0]}
        n = len(symbol_full_list[0])
        
        for i in range(len(symbol_strategy)):
            symbol = symbol_full_list[i]
            for j in range(n):
                try:
                    rank[symbol[j]] += j
                except:
                    continue
        
        # sort in ascending orders so the once with the best rank are in front
        symbol_sorted = [k for k, v in sorted(rank.items(), key=lambda item: item[1])][:n_symbol]
        
        return symbol_sorted
    
    def performance_best(self, df_daily, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir):
        """
        Looks which strategy worked best for prior day and uses it for the current day

        Parameters
        ----------
        df_daily : Dataframe with daily historical prices for performance_sorted() function
        startCap : Start Capital, will only be used if this is the first backtest
        start : start date of backtesting, has to be datetime.date object
        end : end date of backtesting, has to be datetime.date object
        commission : trading fee
        risk_free : risk free market return
        interval : interval of backtesting
        hdfs_path : ...
        sqlContext : ... 

        Returns
        -------
        Writes results into hdfs  /performance_best and /depot_best directory

        """
        # need to add 1 day because the performance_best function uses the results
        # from prior days
        start = start + timedelta(days=1)
        end = end + timedelta(days=1)
        
        weekday_start = start.strftime("%A")
        weekday_end = end.strftime("%A")
            
        if(weekday_start == 'Saturday'):
            start += timedelta(days=2)
        if(weekday_end == 'Saturday'):
            end += timedelta(days=2)
        if(weekday_start == 'Sunday'):
            start += timedelta(days=1)
        if(weekday_end == 'Sunday'):
            start += timedelta(days=1)
        
        # day is the iterator
        day = start
        
        if(day == date(2020,5,25)):
            day += timedelta(days=1)
            
        if(end == date(2020, 5, 25)):
            end += timedelta(days=1)
            
        depotId = self.depotId_func(sqlContext, hdfs_path,"backtest/performance_best"+hdfs_dir)
    
        while(day <= end):
            
            # need prior day to get results from backtesting

            day_prior = day - timedelta(days=1)
                
            if(day_prior == date(2020,5,25)):
                day_prior = date(2020,5,22)
                
                # make sure that it's not trying to stream quote data from weekend
            weekday = day.strftime("%A")
    
            if(weekday == 'Saturday'):
                day += timedelta(days=2)
            if(weekday == 'Sunday'):
                day += timedelta(days=1)
                day_prior -= timedelta(days=1)
            if(weekday == 'Monday'):
                day_prior -= timedelta(days=2)
                
            if(day == date(2020,5,25)):
                day += timedelta(days=1)
            
            # see if there was already backtesting, if yes use value and total number of trades
            try:
                df = sqlContext.read.format('parquet').load(hdfs_path+"/backtest/performance_best"+hdfs_dir).orderBy(F.col("End-Date").desc()).first()
                value = df.Value
                trades = df.Trades
                value_market = df["Value_S&P500"]
            except:
                value = startCap
                trades = 0
                value_market = startCap_market
            
            # read in strategy that had best performance the prior day
            best_performance = sqlContext.read.format('parquet').load(hdfs_path+"/backtest/performance_full"+hdfs_dir+"/"+day_prior.strftime("%Y%m%d")).orderBy(F.col("Performance_Strategy").desc()).head()
            best_depot = sqlContext.read.format('parquet').load(hdfs_path+"/backtest/depot_full"+hdfs_dir+"/"+day_prior.strftime("%Y%m%d")).filter(F.col("DepotId")==best_performance.DepotId).head()
            
            # get attributes
            symbol_strategy = best_depot.Symbol_Strategy
            symbol = self.get_symbol(df_daily, day, symbol_strategy,len(best_depot.ISIN))
            share = best_depot.Share
            strategy = best_depot.Strategy
            
            # market performance and beta of stocks
            print(day, day+timedelta(days=1))
            market = self.market_performance(interval, day, (day + timedelta(days=1)), sqlContext, hdfs_path)
            try:
                beta = YahooFinancials(symbol).get_beta()
            except:
                beta = None
    
            # quote data of the current day
            historical_pd = yf.download(symbol,start = day, end = (day + timedelta(days=1)),interval=interval)
            historical_pd = historical_pd['Close']
            historical_pd.reset_index(drop=False, inplace=True)
            historical_spark = sqlContext.createDataFrame(historical_pd)
            
            # use the best strategy of the prior day for this day
            if(strategy == "Buy and Hold"):
                position = self.buy_and_hold_portfolio_position(historical_spark, symbol, interval, sqlContext)
                performance_data = [self.depot(depotId, symbol, share, position, value, value_market, commission, risk_free, market, beta, trades, True)]
                depot_data = [(depotId,value,"Buy and Hold",symbol,share,symbol_strategy)]
                depotId += 1
    
            if(strategy[:8] == "momentum"):
                position = self.momentum_portfolio_position(historical_spark, symbol, interval, int(strategy[8:]), sqlContext)
                performance_data = [self.depot(depotId, symbol, share, position, value, value_market, commission, risk_free, market, beta, trades, True)]
                depot_data = [(depotId,value,strategy,symbol,share,symbol_strategy)]
                depotId += 1
    
            # write performance and depot data to hdfs
            schema_performance = StructType() \
                    .add("DepotId", LongType())\
                    .add("Value",DoubleType())\
                    .add("Alpha", DoubleType())\
                    .add("Beta", DoubleType())\
                    .add("Start-Capital", DoubleType())\
                    .add("Profit", DoubleType())\
                    .add("Start-Date",DateType())\
                    .add("End-Date",DateType())\
                    .add("Trades", LongType())\
                    .add("Performance_Strategy", DoubleType())\
                    .add("Performance_S&P500", DoubleType())\
                    .add("Value_S&P500", DoubleType())\
                    .add("Start-Capital_S&P500", DoubleType())
            df_performance = sqlContext.createDataFrame(performance_data,schema_performance)
            df_performance.write.save(hdfs_path+"/backtest/performance_best"+hdfs_dir, format='parquet', mode='append')
    
            schema_depot = StructType() \
                    .add("DepotId", LongType())\
                    .add("Start-Capital",DoubleType())\
                    .add("Strategy", StringType())\
                    .add("ISIN", ArrayType(StringType()))\
                    .add("Share", ArrayType(DoubleType()))\
                    .add("Symbol_Strategy", ArrayType(LongType()))
    
            df_depot = sqlContext.createDataFrame(depot_data,schema_depot)
            df_depot.write.save(hdfs_path+"/backtest/depot_best"+hdfs_dir, format='parquet', mode='append')

            day += timedelta(days=1)
    
    
    def full_backtest(self, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, period, n_stock, hdfs_path, sqlContext, best_of, mode):
        
        # get number of days to read in historical data
        day = end + timedelta(days=1)
        n_days = max(period) + (end - start).days
        hdfs_dir_static = ""
        hdfs_dir_ml = "_ml"

        if(mode == "full"):
            # historical data
            df_daily_loop = self.get_historical_daily(None, end, n_days)
            df_minute = self.get_historical_minute(None, start, end)
            df_daily_best = self.get_historical_daily(None, end+timedelta(days=1), n_days+1)
            
            # backtesting with static strategies
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_static, best_of)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_static)
            
            # backtesting with changing strategies
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_ml,best_of)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_ml)
            
        if(mode == "static_full"):
            # historical data
            df_daily_loop = self.get_historical_daily(None, end, n_days)
            df_minute = self.get_historical_minute(None, start, end)
            df_daily_best = self.get_historical_daily(None, end+timedelta(days=1), n_days+1)
            
            # backtesting with static strategies
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_static, best_of)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_static)
            
        if(mode == "static_no_best"):
            # historical data
            df_daily_loop = self.get_historical_daily(None, end, n_days)
            df_minute = self.get_historical_minute(None, start, end)
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_static, best_of)
            
        if(mode == "static_only_best"):
            df_daily_best = self.get_historical_daily(None, end+timedelta(days=1), n_days+1)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_static)
            
        if(mode == "ml_full"):
            # historical data
            df_daily_loop = self.get_historical_daily(None, end, n_days)
            df_minute = self.get_historical_minute(None, start, end)
            df_daily_best = self.get_historical_daily(None, end+timedelta(days=1), n_days+1)
            
            # backtesting with static strategies
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_ml, best_of)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_ml)
            
        if(mode == "ml_no_best"):
            # historical data
            df_daily_loop = self.get_historical_daily(None, end, n_days)
            df_minute = self.get_historical_minute(None, start, end)
            self.performance_loop(df_daily_loop, df_minute, period, startCap, startCap_market, commission, risk_free, strategy, interval, start, end, n_stock, hdfs_path, sqlContext,hdfs_dir_ml, best_of)
            
        if(mode == "ml_only_best"):
            df_daily_best = self.get_historical_daily(None, end+timedelta(days=1), n_days+1)
            self.performance_best(df_daily_best, startCap, startCap_market, start, end, commission, risk_free, interval, hdfs_path, sqlContext, hdfs_dir_ml)
    
class realtime:
    """class to simulate trading with realtime stock data"""
    
    def get_best_performance(self, day, sqlContext):
        """
        Get symbols, share and strategy which worked best the prior day

        Returns
        -------
        list of symbols, share, name of strategy

        """
        hdfs_path = "hdfs://0.0.0.0:19000"
        
        day_prior = day - timedelta(days=1)
        weekday = day_prior.strftime("%A")
        if(weekday == 'Saturday'):
            day_prior -= timedelta(days=1)
        if(weekday == 'Sunday'):
            day_prior -= timedelta(days=2)
    
        best_performance = sqlContext.read.format('parquet').load(hdfs_path+"/backtest/performance_full/"+day_prior.strftime("%Y%m%d")).orderBy(F.col("Performance_Strategy").desc()).head()
        best_depot = sqlContext.read.format('parquet').load(hdfs_path+"/backtest/depot_full/"+day_prior.strftime("%Y%m%d")).filter(F.col("DepotId")==best_performance.DepotId).head()
    
        symbol_strategy = best_depot.Symbol_Strategy
        df_daily = backtest().get_historical_daily(None, day, max(symbol_strategy)+1)
    
        symbol = backtest().get_symbol(df_daily, day, symbol_strategy,len(best_depot.ISIN))
        share = best_depot.Share
        strategy = best_depot.Strategy
        
        return symbol, share, strategy
    
    def write_producer_batch(self,symbol,sandbox):
        """
        Writes script to run kafka producer and stream quotes

        Parameters
        ----------
        symbol : list of symbols

        Returns
        -------
        None.

        """
        txt = ":loop \n"
    
        if(sandbox == True):
            jar = "java -jar iex_kafka_producer-jar-with-dependencies-sandbox.jar \"%s\" \"127.0.0.1:9092\" \"1\" true \n"
        else:
            jar = "java -jar iex_kafka_producer-jar-with-dependencies.jar \"%s\" \"127.0.0.1:9092\" \"1\" true \n"
        
        for symbol in symbol:
            txt += jar % (symbol)
        
        txt += "goto loop" 
        with open("producer.cmd", "w") as f:
                f.write(txt)
                
        start_cmd = "start cmd /k producer.cmd \n"
        with open("producer_start.bat", "w") as f:
                f.write(start_cmd)
                
    def write_producer_batch_parallel(self,symbol,sandbox):
        """
        Writes script to run kafka producer and stream quotes

        Parameters
        ----------
        symbol : list of symbols

        Returns
        -------
        None.

        """
        start_cmd = ""
        
        for symbol in symbol:
            txt = ":loop \n java -jar iex_kafka_producer-jar-with-dependencies.jar \"%s\" \"127.0.0.1:9092\" \"1\" true \n goto loop" % (symbol)
            
            with open("producer"+symbol+".cmd", "w") as f:
                f.write(txt)
                
                start_cmd += "start cmd /k producer"+symbol+".cmd \n"
                
        with open("producer_start.bat", "w") as f:
            f.write(start_cmd)
    
    def buy_and_hold(self, df, symbol, *args):
        """
        Calculates positions for buy and hold strategy

        Parameters
        ----------
        df : Dataframe that cointains Columns "symbol", "Datetime", "latestPrice"
        symbol : Stock ticker
        *args : Does nothing, just necessary for the loop later

        Returns
        -------
        Dataframe with Buy and Hold positions.

        """
        return df.filter(F.col("symbol") == symbol).withColumn("Position", F.lit(1)).orderBy("Datetime",ascending=False)

    def momentum(self, df, symbol, mom):
        df = df.filter(F.col("symbol") == symbol)
        shift = df.select("latestPrice","Datetime")\
                    .withColumn("latestPrice_shift", F.lag(df.latestPrice)\
                    .over(Window.partitionBy().orderBy("Datetime")))
        
        returns = shift\
                    .withColumn("returns",F.log(F.col("latestPrice")/F.col("latestPrice_shift")))
        
        position = returns.\
                    withColumn("Position",F.signum(F.avg("returns")\
                    .over(Window.partitionBy().rowsBetween(-mom,0))))
        
        return position.select("latestPrice","Datetime","Position").orderBy("Datetime", ascending = False).na.drop()
    
    def realtime_init(self, symbol, share, startCap, commission, strategy, sqlContext, sb):
        """
        Initialize variables for realtime trading simulation

        Parameters
        ----------
        symbol : stock ticker
        share : distribution of money
        startCap : start capital
        commission : trading fees
        strategy : trading strategy, has to be array eg.: [momentum,"momentum",10]
        momentum = function that calculates position for momentum strategy
        "momentum" = hdfs directory with realtime data for momentum strategy
        sqlContext : from spark session

        Returns
        -------
        value : value of depot = moneyInvested + moneyFree
        datetime : latest time when stock was bought/sold
        moneyForInvesting_list : list of free money for each stock
        moneyInStocks_list : list of invested money for each stock
        stocksOwned : list of owned shares for each stock
        trades_total : total number of trades

        """
        hdfs_path = "hdfs://0.0.0.0:19000"
        b = backtest()
        df = sqlContext.read.format('parquet').load(hdfs_path+"/realtime"+sb)
        # number of stocks owned for each company
        stocksOwned = []
        # total number of trades
        trades_total = 0
        # money which can be invested
        moneyForInvesting_list = []
        moneyForInvesting = 0
        # money in stocks for indiviudal companys and total
        moneyInStocks_list = []
        moneyInStocks = 0
        # keep track of current date
        datetime = []
        # do a first loop
        for i in range(len(symbol)):
            moneyForInvesting_list.append(startCap*share[i])
            # no stocks in the beginning
            stocksOwned.append(0)
            # no money in stocks in the beginning
            moneyInStocks_list.append(0)
            # filter out right symbol, get latest price
            df_pos = strategy[0](df, symbol[i],strategy[2])
            # collect dataframe to use values in columns
            rows = df_pos.collect()[0]
            # save current datetime to compare with next one
            datetime.append(rows.Datetime)
            result = b.SimulateTrading(moneyForInvesting_list[i], stocksOwned[i], float(rows["latestPrice"]), rows["Position"], commission, trades_total)
            moneyForInvesting_list[i] = result[0]
            stocksOwned[i] = result[1]
            trades_total = result[2]
            # money currently invested in stocks
            moneyInStocks_list[i] = stocksOwned[i]*(float(rows["latestPrice"])-commission)
            moneyInStocks += moneyInStocks_list[i]
            # free money that can be used to buy stocks
            moneyForInvesting += moneyForInvesting_list[i]
            
        value = moneyForInvesting + moneyInStocks
        moneyForInvesting_list = [share[i]*moneyForInvesting for i in range(len(symbol))]
        
        return value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, moneyForInvesting
    
    def realtime_loop(self,value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, symbol, share, commission, strategy, sqlContext, directory):
        """
        After initializing use this function in loop to get realtime trading simulation

        Parameters
        ----------
        value : value of depot = moneyInvested + moneyFree
        datetime : latest time when stock was bought/sold
        moneyForInvesting_list : list of free money for each stock
        moneyInStocks_list : list of invested money for each stock
        stocksOwned : list of owned shares for each stock
        trades_total : total number of trades
        symbol : list of stock tickers
        share : distribution of money
        commission : fee for trading
        strategy : trading strategy, has to be array eg.: [momentum,"momentum",10]
        momentum = function that calculates position for momentum strategy
        "momentum" = hdfs directory with realtime data for momentum strategy
        sqlContext : from spark session

        Returns
        -------
        value : value of depot = moneyInvested + moneyFree
        datetime : latest time when stock was bought/sold
        moneyForInvesting_list : list of free money for each stock
        moneyInStocks_list : list of invested money for each stock
        stocksOwned : list of owned shares for each stock
        trades_total : total number of trades

        """
        hdfs_path = "hdfs://0.0.0.0:19000"
        b = backtest()
        df = sqlContext.read.format('parquet').load(hdfs_path+"/realtime"+directory)
        # current value before trading
        value_init = value
                
        # set to 0 before every trading cycle to calculate new
        moneyInStocks = 0
                
        # cash + whichever money will not be invested
        moneyForInvesting = 0
                
        # go through all companies
        for i in range(len(symbol)):
            # filter out right symbol, get latest price
            df_pos = strategy[0](df, symbol[i],strategy[2])
            rows = df_pos.collect()[0]
            datetime[i] = rows.Datetime
            # rows[0][1] is current price of stock, rows[0][3] is position
            result = b.SimulateTrading(moneyForInvesting_list[i], stocksOwned[i], float(rows["latestPrice"]), rows["Position"], commission, trades_total)
            moneyForInvesting_list[i] = result[0]
            stocksOwned[i] = result[1]
            trades_total = result[2]
            # money currently invested in stocks
            moneyInStocks_list[i] = stocksOwned[i]*(float(rows["latestPrice"])-commission)
            moneyInStocks += moneyInStocks_list[i]
            # free money that can be used to buy stocks
            moneyForInvesting += moneyForInvesting_list[i]
                
        value = moneyForInvesting + moneyInStocks
        moneyForInvesting_list = [share[i]*moneyForInvesting for i in range(len(symbol))]
        
        return value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, moneyForInvesting
    
    def realtime(self, startCap, day, sandbox, commission, sqlContext):
    
        depotid = datetime.now().strftime("%Y%m%d")
        if(sandbox == True):
            es_index = "sb"
        else:
            es_index = ""
            
        # get best parameters from day prior
        best_params = self.get_best_performance(day, sqlContext)
        symbol = best_params[0]
        share = best_params[1]
        strategy_name = best_params[2]
        
        # write and start producer for given symbols
        self.write_producer_batch(symbol, sandbox)
        subprocess.call(['producer_start.bat'])
        time.sleep(5*10*len(symbol))
        
        # get es ready
        es=Elasticsearch([{'host':'localhost','port':9200}])
        
        # initialize strategy
        if(strategy_name == "Buy and Hold"):
            strategy = [self.buy_and_hold,"buyAndHold",10]
        else:
            strategy = [self.momentum, "momentum", int(strategy_name[8:])]
        
        if(sandbox == True):
            directory = "SB"
        else:
            directory = ""
            
        init = self.realtime_init(symbol, share, startCap, commission, strategy, sqlContext, directory)
        value, date, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, cash = init
        
        date_es = date[0]
        # first entry for depot index
        keys_depot = ["value", "date", "depotid", "trades"]
        vals_depot = [value, date_es, depotid, trades_total]
        strategy_dict_depot = dict(zip(keys_depot,vals_depot))
        res_depot = es.index(index="depot"+es_index, body=strategy_dict_depot)
        print(strategy_dict_depot)
        
        # first entry for pie chart index
        pie = [int(x/100) for x in moneyInStocks_list]
        pie.append(int(cash/100))
        pie_labels = [sym for sym in symbol]
        pie_labels.append("cash")
    
        for x in range(len(pie)):
            for y in range(1,pie[x]+1):
                key_pie = ["date","depotid","symbol"]
                vals_pie = [date_es, depotid, pie_labels[x]]
                strategy_dict_pie = dict(zip(key_pie,vals_pie))
                res_pie = es.index(index="pie"+es_index, body=strategy_dict_pie)
                
        while(True):
            last_datetime = date[0]
            
            depot = self.realtime_loop(value, date, moneyForInvesting_list, moneyInStocks_list, stocksOwned,\
                                               trades_total, symbol, share, commission, strategy, sqlContext, directory)
                    
            value, date, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, cash = depot
                
            current_datetime = date[0]
            
            if(last_datetime == current_datetime):
                continue
            else:
                # write values into elasticsearch for visualisation
                date_es = date[0]
                # first entry for depot index
                keys_depot = ["value", "date", "depotid", "trades"]
                vals_depot = [value, date_es, depotid, trades_total]
                strategy_dict_depot = dict(zip(keys_depot,vals_depot))
                res_depot = es.index(index="depot"+es_index, body=strategy_dict_depot)
                print(strategy_dict_depot)
    
                # first entry for pie chart index
                pie = [int(x/100) for x in moneyInStocks_list]
                pie.append(int(cash/100))
                pie_labels = [sym for sym in symbol]
                pie_labels.append("cash")
    
                for x in range(len(pie)):
                    for y in range(1,pie[x]+1):
                        key_pie = ["date","depotid","symbol"]
                        vals_pie = [date_es, depotid, pie_labels[x]]
                        strategy_dict_pie = dict(zip(key_pie,vals_pie))
                        res_pie = es.index(index="pie"+es_index, body=strategy_dict_pie)