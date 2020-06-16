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
```sh
More information and how to run each script is written in source code
```
