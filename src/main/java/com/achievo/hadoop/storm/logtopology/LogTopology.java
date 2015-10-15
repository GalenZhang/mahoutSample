package com.achievo.hadoop.storm.logtopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.achievo.hadoop.storm.logtopology.bolt.IndexerBolt;
import com.achievo.hadoop.storm.logtopology.bolt.LogRulesBolt;
import com.achievo.hadoop.storm.logtopology.bolt.VolumeCountingBolt;
import com.achievo.hadoop.storm.logtopology.common.Conf;
import com.achievo.hadoop.storm.logtopology.spout.LogSpout;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: LogTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: LogTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class LogTopology
{
	private TopologyBuilder builder = new TopologyBuilder();

	private Config conf = new Config();

	private LocalCluster cluster = new LocalCluster();

	@SuppressWarnings("rawtypes")
	public LogTopology()
	{
		builder.setSpout("logSpout", new LogSpout(), 10);
		builder.setBolt("logRules", new LogRulesBolt(), 10).shuffleGrouping("logSpout");
		builder.setBolt("indexer", new IndexerBolt(), 10).shuffleGrouping("logRules");
		builder.setBolt("counter", new VolumeCountingBolt(), 10).shuffleGrouping("logRules");
		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(Conf.LOGGING_KEYSPACE, "",
				Conf.COUNT_CF_NAME, VolumeCountingBolt.FIELD_ROW_KEY, VolumeCountingBolt.FIELD_INCREMENT);
		builder.setBolt("countPersistor", logPersistenceBolt, 10).shuffleGrouping("counter");

		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
		conf.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Conf.LOGGING_KEYSPACE);
	}

	public TopologyBuilder getBuilder()
	{
		return builder;
	}

	public LocalCluster getLocalCluster()
	{
		return cluster;
	}

	public Config getConf()
	{
		return conf;
	}

	public void runLocal(int runTime)
	{
		conf.setDebug(true);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9171");
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0)
		{
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void shutDownLocal()
	{
		if (cluster != null)
		{
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public void runCluster(String name, String redisHost, String cassandraHost) throws AlreadyAliveException,
			InvalidTopologyException
	{
		conf.setNumWorkers(20);
		conf.put(Conf.REDIS_HOST_KEY, redisHost);
		conf.put(StormCassandraConstants.CASSANDRA_HOST, cassandraHost);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		LogTopology topology = new LogTopology();
		if (args != null && args.length > 1)
		{
			topology.runCluster(args[0], args[1], args[2]);
		}
		else
		{
			if (args != null && args.length == 1)
			{
				System.out.println("Running in local mode, redis ip missing for cluster run");
			}
			topology.runLocal(10000);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */