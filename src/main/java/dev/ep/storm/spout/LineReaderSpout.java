package dev.ep.storm.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A spout is a source of streams in a computation. Typically a spout reads from a queueing 
 * broker such as Kestrel, RabbitMQ, or Kafka, but a spout can also generate its own stream 
 * or read from somewhere like the Twitter streaming API. Spout implementations already exist 
 * for most queueing systems.<p>
 * Here, we are reading the input from a text file.
 * @author Surya
 *
 */
public class LineReaderSpout implements IRichSpout {

	private static final long serialVersionUID = -4626423306789663972L;
	private SpoutOutputCollector collector;
	private boolean completed = false;
	private TopologyContext context;
	private String message;
	
	/**
	 * This method would get called at the start and will give you context information. 
	 * You read value of inputFile configuration variable and read that file.
	 */
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		message = conf.get("inputMessage").toString();
	}

	/**
	 * This method would allow you to pass one tuple to storm for processing at a time, 
	 * in this method we are just reading one line from file and pass it to tuple.
	 */
	public void nextTuple() {
		this.collector.emit(new Values(message), message);
	}

	/**
	 * This method declares that LineReaderSpout is going to emit line tuple. 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	public void close() {
		
	}

	public boolean isDistributed() {
		return false;
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}