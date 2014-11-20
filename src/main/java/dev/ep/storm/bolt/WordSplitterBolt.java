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
 * The Class WordSplitterBolt.
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
		String word = "";

		if(sentence.contains("#")){
			word = sentence.substring(sentence.indexOf("#")+1, sentence.length());
			word = word.trim().toUpperCase();
			if(!word.isEmpty()){
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