kafka-storage.sh format -t `kafka-storage.sh random-uuid` -c /opt/kafka/config/kraft/server.properties

kafka-server-start.sh /opt/kafka/config/kraft/server.properties