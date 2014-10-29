package dev.ep.storm.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import dev.ep.storm.bolt.WordCounterBolt;
import dev.ep.storm.bolt.WordSplitterBolt;

public class SimpleHLConsumerKafkaSpout {

	private ConsumerConnector consumer;
	private static final String TOPIC = "TestKafkaTopic";

	public SimpleHLConsumerKafkaSpout() {}

	/** Create the Kafka Consumer for consuming messages from Kafka Producer. */
	public SimpleHLConsumerKafkaSpout(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}


	private void executeConsumer() {
		String strMessage = "";
		Map<String, Integer> topicCount = new HashMap<String, Integer>();

		// Define single thread for topic
		topicCount.put(TOPIC, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(TOPIC);
		
		
		for (final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();

			while (consumerIte.hasNext()) {
				strMessage = new String(consumerIte.next().message());
				System.out.println("Message from Single Topic :: " + strMessage);
				
				processBolt(strMessage);
			}
		}

		if (consumer != null){
			consumer.shutdown();
			
		}
	}
	
	private void processBolt(String strMessage) {
		Config config = new Config();
		config.put("inputMessage", strMessage);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

		TopologyBuilder builder = new TopologyBuilder();
		// Input
		builder.setSpout("line-reader-spout", new LineReaderSpout());

		// Actual processing
		builder.setBolt("word-splitter", new WordSplitterBolt()).shuffleGrouping("line-reader-spout");
		builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-splitter");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		cluster.shutdown();
	}

	public static void main(String[] args) {

		SimpleHLConsumerKafkaSpout simpleHLConsumer = new SimpleHLConsumerKafkaSpout("10.74.230.142:2181", "testgroup");
		simpleHLConsumer.executeConsumer();
	}
}
