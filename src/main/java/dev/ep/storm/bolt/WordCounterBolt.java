package dev.ep.storm.bolt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


/**
 * The Class WordCounterBolt.
 */
public class WordCounterBolt implements IRichBolt {

	/** The output. */
	File fOutput = new File("src/main/resources/Report.html"); 

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 5881928091719450483L;

	/** The counters. */
	Map<String, Integer> counters;

	/** The collector. */
	private OutputCollector collector;

	/** 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
	}

	/** 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple input) {
		// Count Logic
		String str = input.getString(0);
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) +1;
			counters.put(str, c);
		}

		// Print Report
		printReport();

		// Acknowledge
		collector.ack(input);
	}

	/** 
	 * @see backtype.storm.task.IBolt#cleanup()
	 */
	public void cleanup() {

	}

	/** 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	/** 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * Prints the report in HTML format.
	 */
	private void printReport(){
		StringBuffer buffer = new StringBuffer();
		try (PrintWriter writer = new PrintWriter(fOutput)) {
			buffer.append("<html><body>");
			buffer.append("<center>");
			buffer.append("<h3>Report</h3>");
			buffer.append("<table border=\"1\" cellpadding=\"5\">");

			for(Map.Entry<String, Integer> entry: counters.entrySet()){
				buffer.append("<tr>");
				buffer.append("<td>" + entry.getKey() + "</td>" + "<td>" + entry.getValue() + "</td>");
				buffer.append("</tr>");
			}

			buffer.append("</table>");
			buffer.append("</center>");
			buffer.append("</body></html>");

			writer.print(buffer);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}