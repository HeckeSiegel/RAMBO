:loop 
java -jar iex_kafka_producer-jar-with-dependencies.jar "ABBV" "127.0.0.1:9092" "1" true 
java -jar iex_kafka_producer-jar-with-dependencies.jar "ZBH" "127.0.0.1:9092" "1" true 
goto loop