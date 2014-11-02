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
 * A bolt processes any number of input streams and produces any number of new output streams. 
 * Most of the logic of a computation goes into bolts, such as functions, filters, streaming joins,
 * streaming aggregations, talking to databases, and so on.
 */
public class WordCounterBolt implements IRichBolt{
	File fOutput = new File("src/main/resources/Report.html"); 

	private static final long serialVersionUID = 5881928091719450483L;
	Map<String, Integer> counters;
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String str = input.getString(0);
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) +1;
			counters.put(str, c);
		}
		StringBuffer buffer = new StringBuffer();
		try (PrintWriter writer = new PrintWriter(fOutput)) {
			buffer.append("<html><body>");
			buffer.append("<center>");
			buffer.append("<h3>Report</h3>");
			buffer.append("<table border=\"1\" cellpadding=\"5\">");
			
			for(Map.Entry<String, Integer> entry: counters.entrySet()){
				//System.out.println(entry.getKey()+" : " + entry.getValue());
				//writer.println(entry.getKey()+" : " + entry.getValue());
				
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
		collector.ack(input);
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}