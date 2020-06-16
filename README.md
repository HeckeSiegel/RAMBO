<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
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
* closedMarketStream.py : use this between 10 pm and 3:30 pm next day to simulate market data outside of market hours
* openMarketStream.py : use this between 3:30 pm and 10 pm during market hours to get real data
* robo_visor.ipynb : Jupyter notebook with exaples on how to use backtesting and realtime functions
```
More information and how to run each script is written in source code
```
3. Folder "lib" contains sparkStructuredStreaming library with all needed functions 
