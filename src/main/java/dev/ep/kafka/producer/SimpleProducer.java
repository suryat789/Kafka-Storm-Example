package dev.ep.kafka.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	
	private static final String TOPIC = "TestKafkaTopic";
	private static Producer<Integer, String> producer;
	private final Properties props = new Properties();
	private static final File inputFile = new File("src/main/resources/input.txt");
	
	public SimpleProducer()
	{
		props.put("metadata.broker.list", "10.74.230.142:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		producer = new Producer<Integer, String>(new ProducerConfig(props));
	}

	public static void main(String[] args) {
		KeyedMessage<Integer, String> data = null; //new KeyedMessage<Integer, String>(TOPIC, MESSAGE);
		SimpleProducer sp = new SimpleProducer();
		String strData = null;
		
		try (BufferedReader br = new BufferedReader(new FileReader(inputFile))){
			while((strData = br.readLine()) != null){
				data = new KeyedMessage<Integer, String>(TOPIC, strData);
				producer.send(data);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		
		producer.close();
	}
}
