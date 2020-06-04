:loop 
java -jar iex_kafka_producer-jar-with-dependencies.jar "M" "127.0.0.1:9092" "1" true 
java -jar iex_kafka_producer-jar-with-dependencies.jar "DXC" "127.0.0.1:9092" "1" true 
goto loop