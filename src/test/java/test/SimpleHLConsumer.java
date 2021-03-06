package test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import dev.utils.Constants;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class SimpleHLConsumer {
	private final ConsumerConnector consumer;
	//private static final String TOPIC = "TestKafkaTopic";

	public SimpleHLConsumer(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	public void testConsumer() {
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		
		// Define single thread for topic
		topicCount.put(Constants.KAFKA_TOPIC, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(Constants.KAFKA_TOPIC);
		
		for (final KafkaStream stream : streams) {
			ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
			
			while (consumerIte.hasNext())
				System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
		}
		
		if (consumer != null){
			consumer.shutdown();
		}
	}

	public static void main(String[] args) {
		
		SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer("10.0.0.9:2181", "testgroup");
		simpleHLConsumer.testConsumer();
	}
}
