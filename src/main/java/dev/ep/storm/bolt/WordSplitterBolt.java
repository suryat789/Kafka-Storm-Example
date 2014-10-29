package dev.ep.storm.bolt;
import java.util.Map;
 

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * A bolt processes any number of input streams and produces any number of new output streams. 
 * Most of the logic of a computation goes into bolts, such as functions, filters, streaming joins,
 * streaming aggregations, talking to databases, and so on.
 */
public class WordSplitterBolt implements IRichBolt{
	
	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 4341564919704350874L;
	
	/** The collector. */
	private OutputCollector collector;
	
	/**
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	/**
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for(String word: words){
			word = word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		collector.ack(input);
	}
	
	/**
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
 
	/**
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	public void cleanup() {
	}
	
	/**
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}