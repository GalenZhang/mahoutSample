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
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("logSpout", new LogSpout(), 10);
		builder.setBolt("logRules", new LogRulesBolt(), 10).shuffleGrouping("logSpout");
		builder.setBolt("indexer", new IndexerBolt(), 10).shuffleGrouping("logRules");
		builder.setBolt("counter", new VolumeCountingBolt(), 10).shuffleGrouping("logRules");
		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(Conf.LOGGING_KEYSPACE, "",
				Conf.COUNT_CF_NAME, VolumeCountingBolt.FIELD_ROW_KEY, VolumeCountingBolt.FIELD_INCREMENT);
		builder.setBolt("countPersistor", logPersistenceBolt, 10).shuffleGrouping("counter");

		Config conf = new Config();
		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
		conf.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Conf.LOGGING_KEYSPACE);

		if (args != null && args.length > 1)
		{
			conf.setNumWorkers(20);
			conf.put(Conf.REDIS_HOST_KEY, args[1]);
			conf.put(StormCassandraConstants.CASSANDRA_HOST, args[2]);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else
		{
			if (args != null && args.length == 1)
			{
				System.out.println("Running in local mode, redis ip missing for cluster run");
			}

			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			conf.put(Conf.REDIS_HOST_KEY, "localhost");
			conf.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9171");
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */