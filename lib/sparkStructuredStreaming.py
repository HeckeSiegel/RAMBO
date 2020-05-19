from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from pytz import timezone
from pyspark.sql.window import Window
from elasticsearch import Elasticsearch
from yahoofinancials import YahooFinancials
import math as m
from alpha_vantage.sectorperformance import SectorPerformances
import operator
import pandas as pd
from ast import literal_eval

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
            .option("startingOffsets", "earliest") \
            .load()
        
        parsedDF = streamingDF.select(F.col("key").cast("String"), F.col("value").cast("string"))
        
        # this step was necessary because each row contains 10 news
        parsedDF = parsedDF.select(parsedDF.key,\
                                       F.explode(array(F.get_json_object(parsedDF.value, '$[0]'),\
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
            .option("checkpointLocation",hdfs_path +"/checkpoint"+"/"+checkpoint) \
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
            moneyOwned -= (stockPrice*stocksBuy+commission*stocksBuy)
            trades += 1
            stocksOwned += stocksBuy
        elif (position < 0) and (stocksOwned>0) :
        # sell all stocks
            stocksSell = stocksOwned
            moneyOwned += (stocksSell*stockPrice-commission*stocksSell)
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
    
    def buy_and_hold_portfolio_position(self, df, symbol, interval, period, sqlContext, hdfs_path):
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
    
    def momentum_portfolio_position(self, df, symbol, interval, period, momentum, sqlContext):
        
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
    
    
    def depot(self, depotId, symbol, share, position, startCap, commission, risk_free, performance_market, beta_stocks):
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
        trades_total = 0
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
                moneyInStocks_list[i] = (stocksOwned[i]*row[close]-commission*stocksOwned[i])
                moneyInStocks += moneyInStocks_list[i]
                # free money that can be used to buy stocks
                moneyFree += moneyForInvesting[i]
                beta_current += beta_stocks[symbol[i]]*moneyInStocks_list[i]/value
                
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
        beta_avg = sum(beta_list)/len(beta_list)
        # alpha of portfolio
        alpha = performance_depot - rf - beta_avg*(performance_market - rf)
        
        return (depotId,round(value,2),round(alpha,2),round(beta_avg,2), startCap, round(profit,2), start_date, end_date, trades_total, round(performance_depot,2), performance_market)
    
    def market_performance(self, interval, period ,sqlContext, hdfs_path):
        """
        Performance of S&P500 for given interval

        Parameters
        ----------
        interval : granularity of quotes
        sqlContext : from soark session
        hdfs_path : "hdfs://0.0.0.0:19000"

        Returns
        -------
        market performance in %

        """
        if(interval in ["1d","5d","1wk","1mo","3mo","1h"]):
            timestamp = "Date"
        else:
            timestamp = "Datetime"
        
        historical_pd = yf.download("^GSPC",period=period,interval=interval)
        historical_pd.reset_index(drop=False, inplace=True)
        snp500 = sqlContext.createDataFrame(historical_pd)
        
        snp500acs = snp500.orderBy(timestamp)
        first = snp500acs.first()
        snp500desc = snp500.orderBy(F.col(timestamp).desc())
        last = snp500desc.first()
        performance = round((last.Close/first.Close - 1)*100,2)
        return (performance)
    
    def depotId_func(self, sqlContext, hdfs_path):
        """
        Finds out latest depotid

        """
        try:
            df = sqlContext.read.format('parquet').load(hdfs_path+"/performance").orderBy(F.col("DepotId").desc())
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
        equal = [float(1/n) for i in range(n-1)]
        equal.append(float(1) - sum(equal))
    
        desc = [float(i) for i in range(n,0,-1)]
        desc = [float(i/sum(desc)) for i in desc]
        return equal, desc
    
    def performance(self, startCap, commission, risk_free, strategy, interval, period, symbol, n_stock, hdfs_path, sqlContext):
        """
        Calculates performance of given strategy and performance of buy and hold strategy for the same stocks + shares
        so that one can compare if it would have been better to just buy and hold the stocks

        Parameters
        ----------
        depotId : Individual Id for each depot
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
        # get symbol + share
        #symbol_full_list = self.min_pe_symbols(symbol)
        with open("symbol.txt", "r") as f:
            symbol_full_list = f.read()
        symbol_full_list = literal_eval(symbol_full_list)      
        # get market performance
        market = self.market_performance(interval, period, sqlContext, hdfs_path)
        
        # get latest depotId
        depotId = self.depotId_func(sqlContext, hdfs_path)
          
        # get market beta of individual stocks
        beta = YahooFinancials(symbol_full_list[:max(n_stock)]).get_beta()
        if("^GSPC" in symbol_full_list[:max(n_stock)]):
            beta["^GSPC"] = 1.0
                
        for n in n_stock:
            symbol = symbol_full_list[:n]
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
                    depot_data = [(depotId_momentum[0],startCap,strategy[0]+str(strategy[1]),symbol,share)]
                    
                    for i in range(2,len(strategy)):
                        # loop through rest of momentum strategy to get performance for other momentums
                        pos = self.momentum_portfolio_position(historical_spark, symbol, interval, period, strategy[i], sqlContext)
                        position_momentum.append(pos)
                        depotId += 1
                        depotId_momentum.append(depotId)
                        performance_momentum = self.depot(depotId_momentum[i-1], symbol, share, position_momentum[i-1], startCap, commission, risk_free, market, beta)
                        performance_data.append(performance_momentum)
                        depot_momentum = (depotId_momentum[i-1],startCap,strategy[0]+str(strategy[i]),symbol,share)
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
                depot_bnh = (depotId,startCap,"Buy and Hold",symbol,share)
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
                        .add("Start-Caputal",DoubleType())\
                        .add("Strategy", StringType())\
                        .add("ISIN", ArrayType(StringType()))\
                        .add("Share", ArrayType(DoubleType()))
                df_depot = sqlContext.createDataFrame(depot_data,schema_depot)
                df_depot.write.save(hdfs_path+"/depot", format='parquet', mode='append')
        
        return performance_data,depot_data
    
class realtime:
    """class to simulate trading with realtime stock data"""
    
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
    
    def realtime_init(self, symbol, share, startCap, commission, strategy, sqlContext):
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
        df = sqlContext.read.format('parquet').load(hdfs_path+"/realtime")
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
            # rows[0][1] is current price of stock, rows[0][3] is position
            result = b.SimulateTrading(moneyForInvesting_list[i], stocksOwned[i], rows.latestPrice, rows.Position, commission, trades_total)
            moneyForInvesting_list[i] = result[0]
            stocksOwned[i] = result[1]
            trades_total = result[2]
            # money currently invested in stocks
            moneyInStocks_list[i] = (stocksOwned[i]*(rows.latestPrice-commission))
            moneyInStocks += moneyInStocks_list[i]
            # free money that can be used to buy stocks
            moneyForInvesting += moneyForInvesting_list[i]
            
        value = moneyForInvesting + moneyInStocks
        moneyForInvesting_list = [share[i]*moneyForInvesting for i in range(len(symbol))]
        
        return value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total
    
    def realtime_loop(self,value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total, symbol, share, commission, strategy, sqlContext):
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
        df = sqlContext.read.format('parquet').load(hdfs_path+"/realtime")
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
            result = b.SimulateTrading(moneyForInvesting_list[i], stocksOwned[i], rows.latestPrice, rows.Position, commission, trades_total)
            moneyForInvesting_list[i] = result[0]
            stocksOwned[i] = result[1]
            trades_total = result[2]
            # money currently invested in stocks
            moneyInStocks_list[i] = (stocksOwned[i]*(rows.latestPrice-commission))
            moneyInStocks += moneyInStocks_list[i]
            # free money that can be used to buy stocks
            moneyForInvesting += moneyForInvesting_list[i]
                
        value = moneyForInvesting + moneyInStocks
        moneyForInvesting_list = [share[i]*moneyForInvesting for i in range(len(symbol))]
        
        return value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total
    
    def realtime(self, symbol, share, startCap, commission, strategy, index, sqlContext):
        
        es=Elasticsearch([{'host':'localhost','port':9200}])
        # compare momentum strategy with buy and hold
        strategy_b = [self.buy_and_hold,"buyAndHold",10]
        # initialize both strategys
        init_strategy = self.realtime_init(symbol, share, startCap, commission, strategy, sqlContext)
        init_b = self.realtime_init(symbol, share, startCap, commission, strategy_b, sqlContext)
        value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total = init_strategy
        value_b, datetime_b, moneyForInvesting_list_b, moneyInStocks_list_b, stocksOwned_b, trades_total_b = init_b
        
        while(True):
            last_datetime = datetime[0]
            # update depot and stocksOwned in loop until break
            depot_strategy = self.realtime_loop(value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned,\
                                           trades_total, symbol, share, commission, strategy, sqlContext)
            depot_b = self.realtime_loop(value_b, datetime_b, moneyForInvesting_list_b, moneyInStocks_list_b, stocksOwned_b,\
                                      trades_total_b, symbol, share, commission, strategy_b, sqlContext)
                
            value, datetime, moneyForInvesting_list, moneyInStocks_list, stocksOwned, trades_total = depot_strategy
            value_b, datetime_b, moneyForInvesting_list_b, moneyInStocks_list_b, stocksOwned_b, trades_total_b = depot_b
            
            current_datetime = datetime[0]
            if(last_datetime == current_datetime):
                print("no new data")
                continue
            else:
                print(datetime[0],value,value_b)
                # write values into elasticsearch for visualisation
                doc = {}
                doc['date'] = datetime[0]
                doc['value_momentum'] = value
                doc['value_buy_and_hold'] = value_b
                res = es.index(index=index, body=doc)