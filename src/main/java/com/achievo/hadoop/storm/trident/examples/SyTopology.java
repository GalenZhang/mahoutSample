package com.achievo.hadoop.storm.trident.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SyTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SyTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SyTopology
{
	private final BrokerHosts brokerHosts;

	public SyTopology(String kafkaZookeeper)
	{
		this.brokerHosts = new ZkHosts(kafkaZookeeper);
	}

	public StormTopology buildTopology()
	{
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, "ma30", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
		TridentTopology topology = new TridentTopology();

		topology.newStream("kafka4", kafkaSpout).each(new Fields("str"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
				.parallelismHint(16);

		return topology.build();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		String kafkaZk = args[0];
		SyTopology topology = new SyTopology(kafkaZk);
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		String name = args[1];
		String dockerIp = args[2];
		config.setNumWorkers(9);
		config.setMaxTaskParallelism(5);
		config.put(Config.NIMBUS_HOST, dockerIp);
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));

		StormSubmitter.submitTopology(name, config, topology.buildTopology());
	}

	static class Split extends BaseFunction
	{
		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = -4448893385104673521L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector)
		{
			String sentence = tuple.getString(0);
			for (String word : sentence.split(","))
			{
				try
				{
					FileWriter fw = new FileWriter(new File("/home/hadoop/test/ma30/ma30.txt"), true);
					fw.write(word);
					fw.flush();
					fw.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}

				collector.emit(new Values(word));
			}
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */