package com.achievo.hadoop.storm;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

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
 *  File: DistributeWordTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: DistributeWordTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 11, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class DistributeWordTopology
{
	public static class KafkaWordToUpperCase extends BaseRichBolt
	{

		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = -3226415809809587644L;

		private static final Log LOG = LogFactory.getLog(KafkaWordToUpperCase.class);

		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input)
		{
			String line = input.getString(0).trim();
			LOG.info("RECV[kafka -> splitter] " + line);
			if (!line.isEmpty())
			{
				String upperLine = line.toUpperCase();
				LOG.info("EMIT[splitter -> counter] " + upperLine);
				collector.emit(input, new Values(upperLine, upperLine.length()));
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer)
		{
			declarer.declare(new Fields("line", "len"));
		}

	}

	public static class RealtimeBolt extends BaseRichBolt
	{

		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = -5706795362128489890L;

		private static final Log LOG = LogFactory.getLog(KafkaWordToUpperCase.class);

		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
		{
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input)
		{
			String line = input.getString(0).trim();
			LOG.info("REALTIME: " + line);
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer)
		{

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException
	{
		// Configure Kafka
		String zks = "localhost:2181";
		String topic = "my-replicated-topic5";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "word";
		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.forceFromStart = false;
		spoutConf.zkServers = Arrays.asList(new String[] {"localhost"});
		spoutConf.zkPort = 2181;

		// Configure HDFS bolt
		// use "\t" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\t");
		SyncPolicy syncPolicy = new CountSyncPolicy(1000); // sync the filesystem after every 1k tuples
		FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES); // rotate files
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/storm/").withPrefix("app_")
				.withExtension(".log"); // set file name format

		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://localhost:9000").withFileNameFormat(fileNameFormat)
				.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

		// configure & build topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5);
		builder.setBolt("to-upper", new KafkaWordToUpperCase(), 3).shuffleGrouping("kafka-reader");
		builder.setBolt("hdfs-bolt", hdfsBolt, 2).shuffleGrouping("to-upper");
		builder.setBolt("realtime", new RealtimeBolt(), 2).shuffleGrouping("to-upper");

		// submit topology
		Config conf = new Config();
		String name = DistributeWordTopology.class.getSimpleName();
		if (args != null && args.length > 0)
		{
			String nimbus = args[0];
			conf.put(Config.NIMBUS_HOST, nimbus);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		}
		else
		{
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */