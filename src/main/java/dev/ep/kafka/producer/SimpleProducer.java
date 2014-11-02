package dev.ep.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import dev.utils.Constants;

public class SimpleProducer {

	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();
	private static List<String> nums = new ArrayList<String>();
	private static Random random = new Random();


	public SimpleProducer()	{
		props.put("metadata.broker.list", Constants.KAFKA_HOST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	private void loadProperties(){
		
	}

	public static void main(String[] args) {
		int index = 0;
		loadDummyData();
		KeyedMessage<Integer, String> data = null; //new KeyedMessage<Integer, String>(TOPIC, MESSAGE);
		SimpleProducer sp = new SimpleProducer();
		sp.loadProperties();
		String strData = null;

		for(int i=1; i<=2000; i++){
			index = random.nextInt(nums.size());
			strData = "I love #Windows " + nums.get(index);

			data = new KeyedMessage<Integer, String>(Constants.KAFKA_TOPIC, strData);
			producer.send(data);
		}	
		producer.close();
	}


	public static void loadDummyData() {
		nums.add("98");
		nums.add("XP");
		nums.add("Vista");
		nums.add("7");
		nums.add("8");
	}
}
