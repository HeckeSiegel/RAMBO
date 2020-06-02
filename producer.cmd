:loop 
java -jar iex_kafka_producer-jar-with-dependencies-sandbox.jar "PSX" "127.0.0.1:9092" "1" true 
java -jar iex_kafka_producer-jar-with-dependencies-sandbox.jar "PCG" "127.0.0.1:9092" "1" true 
goto loop