Kafka-Storm-Example
===================

This project demonstrates the Kafka and Storm Integration.

Apache Kafka
=============
Apache Kafka is publish-subscribe messaging open-sourced by LinkedIn. It internally uses ZooKeeper for orchestration.

Apache Storm
=============
Apache Storm is a free and open source distributed realtime computation system. Storm makes it easy to reliably process unbounded streams of data, doing for realtime processing what Hadoop did for batch processing.
It was open-sourced by Twitter.

Test
=====
To test, execute SimpleProducer class. It produces streams of "I love #Windows " preceded by version. This will be the producer.

For consuming messages and processing and further generating a report, we will execute a topology which will keep on executing.
Execute the KafkaTopology class. We have defined bolts here which are basically operations we want to perform on streams of data.

