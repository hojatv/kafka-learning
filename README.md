# Starting Kafka up:
1- start zookeeper : zookeeper-server-start.sh config/zookeeper.properties
2- start Kafka: kafka-server-start.sh config/server.properties
In summary, for Mac OS X
1. Install brew if needed: /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
2. Download and Setup Java 11 JDK:
    * 		brew tap caskroom/versions
    * 		brew cask install java11
3.   
4. Download & Extract the Kafka binaries from https://kafka.apache.org/downloads
5. Install Kafka commands using brew: brew install kafka
6. Try Kafka commands using kafka-topics (for example)
7. Edit Zookeeper & Kafka configs using a text editor
    1. zookeeper.properties: dataDir=/your/path/to/data/zookeeper
    2. server.properties: log.dirs=/your/path/to/data/kafka
8. Start Zookeeper in one terminal window: zookeeper-server-start config/zookeeper.properties
9. Start Kafka in another terminal window: kafka-server-start config/server.properties


# Create a topic :
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1

Get list of the topics:
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe a topic:
kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --describe


Kafka producer console:   kafka-console-producer.sh --bootstrap-server localhost:9092 --topic first_topic

Kafka consumer console:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning (all messages)

# Kafka Consumer Groups
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
Terrific is distributed between these three

Note:
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-secon-application --from-beginning. ==> sees all messages
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-secon-application --from-beginning  ==> running again doesn’t see anymore from beginning since Kafka commits offset for each consumer group
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group my-third-application --from-beginning ==> it is a new consumer group, it sees all messages from beginning

kafka-consumer-groups command:
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

Replaying data : Reset-ofsets :
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --execute --topic first_topic











