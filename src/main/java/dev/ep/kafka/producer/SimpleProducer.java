package dev.ep.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import dev.utils.Constants;

/**
 * The Class SimpleProducer.
 */
public class SimpleProducer {

	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();
	private static List<String> nums = new ArrayList<String>();
	private static Random random = new Random();

	/**
	 * Instantiates a new simple producer.
	 */

	/**
	 * Load properties.
	 */
	private void loadProperties(String strKafkaHosts){
		props.put("metadata.broker.list", strKafkaHosts);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		int index = 0;
		String strData = null;
		SimpleProducer sp = new SimpleProducer();

		sp.loadProperties(Constants.KAFKA_HOST1 + Constants.HOST_SEPARATOR + Constants.KAFKA_HOST2);
		loadDummyData();

		KeyedMessage<Integer, String> data = null; //new KeyedMessage<Integer, String>(TOPIC, MESSAGE);

		for(int i=1; i<=200; i++){
			index = random.nextInt(nums.size());
			strData = "I love #Windows " + nums.get(index);
			System.out.println(i + "...");
			data = new KeyedMessage<Integer, String>(Constants.KAFKA_TOPIC, strData);
			try {
				producer.send(data);
			} catch (Exception ex){
				ex.printStackTrace();
			}
		}
		producer.close();
	}


	/**
	 * Load dummy data.
	 */
	public static void loadDummyData() {
		nums.add("98");
		nums.add("XP");
		nums.add("Vista");
		nums.add("7");
		nums.add("8");
	}
}
