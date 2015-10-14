package com.achievo.hadoop.storm.ordertopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.achievo.hadoop.storm.ordertopology.bolt.ConfidenceComputeBolt;
import com.achievo.hadoop.storm.ordertopology.bolt.FilterBolt;
import com.achievo.hadoop.storm.ordertopology.bolt.PairCountBolt;
import com.achievo.hadoop.storm.ordertopology.bolt.PairTotalCountBolt;
import com.achievo.hadoop.storm.ordertopology.bolt.SplitBolt;
import com.achievo.hadoop.storm.ordertopology.bolt.SupportComputeBolt;
import com.achievo.hadoop.storm.ordertopology.common.ConfKeys;
import com.achievo.hadoop.storm.ordertopology.common.FieldNames;
import com.achievo.hadoop.storm.ordertopology.spout.CommandSpout;
import com.achievo.hadoop.storm.ordertopology.spout.OrderSpout;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: OrderTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: OrderTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class OrderTopology
{
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("orderSpout", new OrderSpout(), 5);
		builder.setSpout("commandSpout", new CommandSpout(), 1);

		builder.setBolt("splitBolt", new SplitBolt(), 5).fieldsGrouping("orderSpout", new Fields(FieldNames.ID));
		builder.setBolt("pairCountBolt", new PairCountBolt(), 5).fieldsGrouping("splitBolt",
			new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
		builder.setBolt("pairTotalCountBolt", new PairTotalCountBolt(), 1).globalGrouping("splitBolt");

		builder.setBolt("supportComputeBolt", new SupportComputeBolt(), 5).allGrouping("pairTotalCountBolt")
				.fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.allGrouping("commandSpout");
		builder.setBolt("confidenceComputeBolt", new ConfidenceComputeBolt(), 5)
				.fieldsGrouping("pairCountBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.allGrouping("commandSpout");

		builder.setBolt("filterBolt", new FilterBolt())
				.fieldsGrouping("supportComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2))
				.fieldsGrouping("confidenceComputeBolt", new Fields(FieldNames.ITEM1, FieldNames.ITEM2));

		Config conf = new Config();
		if (args != null && args.length > 1)
		{
			conf.setNumWorkers(20);
			conf.put(ConfKeys.REDIS_HOST, args[1]);
			conf.put(ConfKeys.REDIS_PORT, "6379");
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else
		{
			System.out.println("Running in local mode" + "\nRedis ip missing for cluster run");
			conf.setDebug(true);
			conf.put(ConfKeys.REDIS_HOST, "localhost");
			conf.put(ConfKeys.REDIS_PORT, "6379");
			LocalCluster cluster = new LocalCluster();
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