package com.achievo.hadoop.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: MyKafkaTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: MyKafkaTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 11, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class MyKafkaTopology
{
	private static class KafkaWordSplitter extends BaseRichBolt
	{

		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = -2548125783528530146L;

		private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);

		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input)
		{
			String line = input.getString(0);
			LOG.info("RECV[kafka -> splitter] " + line);
			for (String word : line.split("\\s+"))
			{
				LOG.info("EMIT[splitter -> counter] " + word);
				collector.emit(input, new Values(word, 1));
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer)
		{
			declarer.declare(new Fields("word", "count"));
		}

	}

	public static class WordCounter extends BaseRichBolt
	{

		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = 5143684009994677152L;

		private static final Log LOG = LogFactory.getLog(WordCounter.class);

		private OutputCollector collector;

		private Map<String, AtomicInteger> counterMap;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			this.collector = collector;
			this.counterMap = new HashMap<String, AtomicInteger>();
		}

		@Override
		public void execute(Tuple input)
		{
			String word = input.getString(0);
			int count = input.getInteger(1);
			LOG.info("RECV[splitter -> counter] " + word + " : " + count);
			AtomicInteger ai = this.counterMap.get(word);
			if (ai == null)
			{
				ai = new AtomicInteger();
				this.counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			collector.ack(input);
			LOG.info("CHECK statistics map: " + this.counterMap);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer)
		{
			declarer.declare(new Fields("word", "count"));
		}

		@Override
		public void cleanup()
		{
			LOG.info("The final result: ");
			Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
			while (iter.hasNext())
			{
				Entry<String, AtomicInteger> entry = iter.next();
				LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
			}
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException
	{
		String zks = "localhost:2181";
		String topic = "my-replicated-topic5";
		String zkRoot = "/storm";
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = false;
		spoutConf.zkServers = Arrays.asList(new String[] {"localhost"});
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).shuffleGrouping("kafka-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

		Config conf = new Config();
		String name = MyKafkaTopology.class.getSimpleName();
		if (args != null && args.length > 0)
		{
			// Nimbus host name passed from command line
			conf.put(Config.NIMBUS_HOST, args[0]);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		}
		else
		{
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(120000);
			cluster.shutdown();
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */