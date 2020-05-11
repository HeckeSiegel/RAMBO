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
from yahoofinancials import YahooFinancials
import math as m

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
        ticker = yf.Ticker(symbol)
        pandas_history = ticker.history(period=period, interval=interval)
        pandas_history.reset_index(drop=False, inplace=True)
        spark_history = sqlContext.createDataFrame(pandas_history)
        spark_history.select("Datetime","Open","High","Close","Volume")\
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
        
        if (position < 0) and (moneyOwned>(stockPrice+commission)) :
        # buy as many stocks as possible stock
            stocksBuy = int(moneyOwned / stockPrice)
            moneyOwned -= (stockPrice*stocksBuy+commission*stocksBuy)
            trades += stocksBuy
            stocksOwned += stocksBuy
        elif (position > 0) and (stocksOwned>0) :
        # sell all stocks
            stocksSell = stocksOwned
            moneyOwned += (stocksSell*stockPrice-commission*stocksSell)
            trades += stocksSell
            stocksOwned = 0
        return (moneyOwned, stocksOwned, trades)
    
    def momentum_position(self, symbol, interval, momentum, sqlContext, hdfs_path):
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
        data_momentum = history().from_hdfs(symbol, interval, sqlContext, hdfs_path)
            
        momentum_shift = data_momentum.select("Close","Datetime")\
                .withColumn("Close_shift", lag(data_momentum.Close)\
                .over(Window.partitionBy().orderBy("Datetime")))
                    
        momentum_returns = momentum_shift\
                .withColumn("returns",log(momentum_shift.Close/momentum_shift.Close_shift))
                
        momentum_df = momentum_returns.\
                withColumn("position",signum(avg("returns")\
                .over(Window.partitionBy().rowsBetween(-momentum,0))))
        close_new = 'Close'+symbol
        position_new = 'Position'+symbol
        
        return momentum_df.select("Datetime",momentum_df['Close'].alias(close_new),momentum_df['position'].alias(position_new)).na.drop()
    
    def buy_and_hold_position(self, symbol, interval, sqlContext, hdfs_path):
        """
        

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
        data = history().from_hdfs(symbol, interval, sqlContext, hdfs_path)
        data_close = data.select("Datetime","Close").orderBy("Datetime")
        position = "Position"+symbol
        close = "Close"+symbol
        data_position = data_close.withColumn(position, lit(-1))
        
        return data_position.withColumnRenamed("Close",close)
    
    def momentum_portfolio_position(self, symbol, interval, momentum, sqlContext, hdfs_path):
        """
        Getting the positions for the momentum strategy of a whole portfolio

        Parameters
        ----------
        symbol : stock ticker
        interval : granularity of stock prices
        momentum : parameter of momentum strategy
        sqlContext : from spark session
        hdfs_path : local -> "hdfs://0.0.0.0:19000"

        Returns
        -------
        df : dataframe with all closing prices and positions

        """
        # dataframe for positions of momentum strategy
        df = self.momentum_position(symbol[0], interval, momentum, sqlContext, hdfs_path)
        # join with nasdaq data to calculate alpha
        nasdaq = self.momentum_position("^IXIC", interval, momentum, sqlContext, hdfs_path)
        df = df.join(nasdaq,"Datetime",how='left')
        
        for i in range(1,len(symbol)):
            df0 = self.momentum_position(symbol[i], interval, momentum, sqlContext, hdfs_path)
            df = df.join(df0,"Datetime",how='left')
        
        df = df.na.drop()
        
        return df
    
    def buy_and_hold_portfolio_position(self, symbol, interval, sqlContext, hdfs_path):
        """
        Positions for buy and hold strategy for a whole portfolio.
        Parameters
        ----------
        symbol : List of symbols in portfolio
        interval : granularity of stock prices
        sqlContext : from spark session
        hdfs_path : local -> "hdfs://0.0.0.0:19000"

        Returns
        -------
        df : joined data frame with positions for all stock symbols + nasdaq index

        """
        # dataframe for positions of momentum strategy
        df = self.buy_and_hold_position(symbol[0], interval, sqlContext, hdfs_path)
        # join with nasdaq data to calculate alpha
        nasdaq = self.buy_and_hold_position("^IXIC", interval, sqlContext, hdfs_path)
        df = df.join(nasdaq,"Datetime",how='left')
        
        for i in range(1,len(symbol)):
            df0 = self.buy_and_hold_position(symbol[i], interval, sqlContext, hdfs_path)
            df = df.join(df0,"Datetime",how='left')
        
        df = df.na.drop()
        
        return df
    
    def depot(self, depotId, symbol, share, position, startCap, commission, risk_free):
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
        # cash which shall not be invested
        cash = startCap*share[-1]
        # dictionary with betas for all symbols
        beta_stocks = YahooFinancials(symbol).get_beta()
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
                nasdaq_init = row["Close^IXIC"]
                
            # current value before trading
            value_init = value
            
            # set to 0 before every trading cycle to calculate new
            moneyInStocks = 0
            
            # cash + whichever money will not be invested
            moneyFree = cash
            
            # calculate beta for each cash distribution new
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
            # distribution of free money has to be distributed new after every trade
            cash = share[-1]*moneyFree
            moneyForInvesting = [share[i]*moneyFree for i in range(len(symbol))]
            beta_list.append(beta_current)
            nasdaq_current = row["Close^IXIC"]
            end_date = row.Datetime
        
        # number of days this strategy was tested
        time = end_date - start_date
        days = m.ceil(time.total_seconds()/(60*60*24))
        rf = days * (m.pow(risk_free+1,1/30) - 1) * 100
        # profit of strategy in $
        profit = value - startCap
        # performance in %
        performance_depot = (value/startCap - 1)*100
        # performance of nasdaq index in %
        performance_nasdaq = (nasdaq_current/nasdaq_init - 1)*100
        # average beta
        beta_avg = m.fsum(beta_list)/len(beta_list)
        # alpha of portfolio
        alpha = performance_depot - rf - beta_avg*(performance_nasdaq - rf)
        
        return (depotId,value,alpha,beta_avg, startCap, profit, start_date, trades_total, performance_depot, performance_nasdaq)
    
    def performance(self, depotId, symbol, share, startCap, commission, risk_free, strategy, interval, hdfs_path, sqlContext):
        """
        Calculates performance of given strategy and performance of buy and hold strategy for the same stocks + shares

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
        if(strategy[0] == "momentum"):
            # calculate positions for momentum strategy
            position1 = [self.momentum_portfolio_position(symbol, interval, strategy[1], sqlContext, hdfs_path)]
            for i in range(2,len(strategy)):
                pos = self.momentum_portfolio_position(symbol, interval, strategy[i], sqlContext, hdfs_path)
                position1.append(pos)
                
        # calculate performance for momentum strategy        
        depotId1 = [depotId]
        performance_data = [self.depot(depotId1[0], symbol, share, position1[0], startCap, commission, risk_free)]
        depot_data = [(depotId1[0],startCap,strategy[0]+str(strategy[1]),symbol,share)]
        for i in range(1,len(position1)):
            depotId += 1
            depotId1.append(depotId)
            depot1 = self.depot(depotId1[i], symbol, share, position1[i], startCap, commission, risk_free)
            performance_data.append(depot1)
            data = (depotId1[i],startCap,strategy[0]+str(strategy[i+1]),symbol,share)
            depot_data.append(data)
         
        # performance for buy and hold strategy
        depotId2 = depotId + 1
        position2 = self.buy_and_hold_portfolio_position(symbol, interval, sqlContext, hdfs_path)
        depot2 = self.depot(depotId2, symbol, share, position2, startCap, commission, risk_free)
        performance_data.append(depot2)
        data = (depotId2,startCap,"Buy and Hold",symbol,share)
        depot_data.append(data)
        
        # write performance and depot data to hdfs
        schema_performance = StructType() \
                .add("DepotId", LongType())\
                .add("Value",DoubleType())\
                .add("Alpha", DoubleType())\
                .add("Beta", DoubleType())\
                .add("Start-Capital", DoubleType())\
                .add("Profit", DoubleType())\
                .add("Start-Date",DateType())\
                .add("Trades", LongType())\
                .add("Performance_Strategy", DoubleType())\
                .add("Performance_Nasdaq", DoubleType())
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