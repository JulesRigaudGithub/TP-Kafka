kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions 1 \
        --topic lines-stream

kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions 1 \
        --topic words-stream
        
kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions 1 \
        --topic tagged-words-stream

kafka-topics.sh --create --bootstrap-server localhost:9092 \
        --replication-factor 1 --partitions 1 \
        --topic command-topic

java -cp target/tp-kafka-0.0.1-SNAPSHOT.jar if4030.kafka.LineParser&
java -cp target/tp-kafka-0.0.1-SNAPSHOT.jar if4030.kafka.LexicalAnalyser&
java -cp target/tp-kafka-0.0.1-SNAPSHOT.jar if4030.kafka.WordCount