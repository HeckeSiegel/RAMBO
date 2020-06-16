<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
* [Getting Started](#getting-started)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
* [Usage](#usage)


<!-- ABOUT THE PROJECT -->
## About The Project

Spark Structured Streaming application to analyze financial market data. 

1. Folder "examples" contains several python scripts that can be used to stream data from Kafka topics and write them into different sinks:
* alphaVantageSector.py : get sector data from AlphaVantage, write into elasticsearch
* elasticSearch.py : stream from all topics simultaneously, write them into elasticsearch
* sparkStructuredStreaming_company.py : stream from topic "company" write into either console, hdfs or elasticsearch sink
* sparkStructuredStreaming_news.py : stream from topic "news", do sentiment analysis, write into either console, hdfs or elasticsearch sink
* sparkStructuredStreaming_quotes.py : stream from topic "quotes" write into either console, hdfs or elasticsearch sink
2. Folder "robo_visor" contains the Robovisor use case, which trades in the stock maket using different strategies:
* iex_kafka_producer-jar-with-dependencies.jar : kafka producer to stream real data
* iex_kafka_producer-jar-with-dependencies-sandbox.jar : kafka producer to stream fake data
* closedMarketStream.py : use this between 10 pm and 3:30 pm next day to simulate market data outside of market hours
* openMarketStream.py : use this between 3:30 pm and 10 pm during market hours to get real data
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
