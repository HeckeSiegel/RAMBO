<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
* [Usage](#usage)
* [Examples](#usage)


<!-- ABOUT THE PROJECT -->
## About The Project

Spark Structured Streaming application to analyze financial market data (Quotes, Newsfeeds, Financial Statements) in realtime to support decision processes for fund and portfolio managers (Buy-Side in Financial Markets).

1. Folder "examples" contains several python scripts that can be used to stream data from Kafka topics and write them into different sinks:
* alphaVantageSector.py : get sector data from AlphaVantage, write into elasticsearch
* elasticSearch.py : stream from all topics simultaneously, write them into elasticsearch
* sparkStructuredStreaming_company.py : stream from topic "company" write into either console, hdfs or elasticsearch sink
* sparkStructuredStreaming_news.py : stream from topic "news", do sentiment analysis, write into either console, hdfs or elasticsearch sink
* sparkStructuredStreaming_quotes.py : stream from topic "quotes" write into either console, hdfs or elasticsearch sink
2. Folder "robo_visor" contains the Robovisor use case, which trades in the stock maket using different strategies:
* iex_kafka_producer-jar-with-dependencies.jar : kafka producer to stream real data
* iex_kafka_producer-jar-with-dependencies-sandbox.jar : kafka producer to stream fake data
* closedMarketStream.py : streams from all topics, writes some of them directly into elasticsearch, writes others into hdfs for realtime trading. Use this between 10 pm and 3:30 pm next day to simulate market data outside of market hours
* openMarketStream.py : Same as closedMarketStream.py, but use this between 3:30 pm and 10 pm during market hours to get real data
* robo_visor.ipynb : Jupyter notebook with exaples on how to use backtesting and realtime functions
```
More information and how to run each script is written in each source code respectively
```
* producer.cmd : runs jar's in loop
* producer_start.bat : runs producer.cmd in new command window
```
function realtime will write and run these automatically
```
3. Folder "lib" (which needs to be in "examples" and "robo_visor") contains sparkStructuredStreaming library with all needed functions 

<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

* Hadoop 2.9.1 or higher
* Kafka 2.11 or higher
* Spark 2.4.5 or higher
* Elasticsearch 7.6.2 or higher
* Elasticsearch-Hadoop 7.6.2 or higher
* Kibana 7.6.2 or higher
* Python 3.7 or higher

<!-- USAGE EXAMPLES -->
## Usage

* If you want to run e.g. sparkStructuredStreaming_quotes.py:
1. Choose in source coude with e.g. Spyder the desired output sink (hdfs, console or elasticsearch -> more than 1 is possible)
2. Open command prompt
3. cd into folder "examples"
4. type in : spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar sparkStructuredStreaming_quotes.py "127.0.0.1:9092" (replace "C:" with the path to your elasticsearch-hadoop directory)
```
note: this only works if you added python to your windows path before, otherwise just use e.g. Anaconda Prompt
```

* If you want to run the Robovisor:
1. Open robo_visor.ipynb in jupyter notebook
2. Do backtesting with desired parameters
3. Open command prompt and cd into "robo_visor" folder
4. Type in spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --jars C:\elasticsearch-hadoop-7.6.2\dist\elasticsearch-spark-20_2.11-7.6.2.jar openMarketStream.py (if market is open, otherwise use closedMarketStream.py)
5. When marketStream is running you can run the function realtime from jupyter notebook to simulate trading

<!-- EXAMPLES -->
## Examples
All of these examples are functions from the robovisor folder.

### Backtesting
- Parameters you can change: start capital, start- and end-date of backtesting, number of companies in portfolio, minutes for momentum strategy
- Automatically also backtests "buy and hold" strategy for same stocks and compares it with momentum strategies
- Produces following lists and saves them into hdfs in folder /backtest/performance_full/yyyymmdd (eg. 20200507) and /backtest/depot_full/yyyymmdd

![backtest](https://github.com/HeckeSiegel/RAMBO/blob/master/pictures/backtest.png)

### Trading Simulation
- The backtesting function also automatically runs a trading simulation during the specified time frame, where for each day it chooses the strategy which worked best from the day before and uses this to trade during this day
- Calculates the current value of the portfolio for each day and compares it to the S&P500 market value
- Keeps track of how many trades have been made each day

![simulation](https://github.com/HeckeSiegel/RAMBO/blob/master/pictures/simulation.png)
