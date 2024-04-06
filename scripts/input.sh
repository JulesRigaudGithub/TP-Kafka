head -100 Les_Trois_Mousquetaires.txt | kafka-console-producer.sh --bootstrap-server localhost:9092 \
        --topic lines-stream
