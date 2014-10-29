package test;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	private static final String TOPIC = "TestKafkaTopic";
	private static final String MESSAGE	= "Hello World On " + new Date().toString(); 
	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();

	public SimpleProducer()
	{
		// props.put("broker.list", "10.74.230.142:9092");
		props.put("metadata.broker.list", "10.74.230.142:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	public static void main(String[] args) {
		SimpleProducer sp = new SimpleProducer();
		KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>(TOPIC, MESSAGE);
		producer.send(data);
		producer.close();
	}
}
