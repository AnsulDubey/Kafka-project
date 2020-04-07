from modules.consumer import start_consumer
from modules.producer import start_producer

# Start ZooKeeper Server
# bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Server
# bin/kafka-server-start.sh config/server.properties

# created kafka topic-
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --topic topic1


if __name__ == "__main__":
    start_producer()
    start_consumer()
