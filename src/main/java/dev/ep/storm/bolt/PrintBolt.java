package dev.ep.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class PrintBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void cleanup() {

	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println("##################################################################################" + tuple.getMessageId());
	}

	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
