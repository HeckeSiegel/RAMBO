:loop
java -jar C:\Users\LEGLES\RAMBO\kafka_api-master\kafka-iex-producer\target\iex_kafka_producer-jar-with-dependencies.jar "AAPL" "127.0.0.1:9092" "1"
timeout /t 10
goto loop