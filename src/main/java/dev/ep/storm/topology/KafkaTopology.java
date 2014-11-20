package dev.ep.storm.topology;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import dev.ep.storm.bolt.WordCounterBolt;
import dev.ep.storm.bolt.WordSplitterBolt;
import dev.utils.Constants;

public class KafkaTopology {

	/**
	 * The main method.
	 * @param args the arguments
	 */
	public static void main(String args[]) {

		BrokerHosts zk = new ZkHosts(Constants.ZK_HOST);
		SpoutConfig spoutConf = new SpoutConfig(zk, Constants.KAFKA_TOPIC, "/kafkastorm", "discovery");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = true;

		KafkaSpout spout = new KafkaSpout(spoutConf);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", spout, 1);

		builder.setBolt("splitterbolt", new WordSplitterBolt()).shuffleGrouping("spout");
		builder.setBolt("countbolt", new WordCounterBolt()).fieldsGrouping("splitterbolt", new Fields("word"));

		Config config = new Config();
		config.setDebug(true);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka", config, builder.createTopology());

		try {
			Thread.sleep(100000000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}