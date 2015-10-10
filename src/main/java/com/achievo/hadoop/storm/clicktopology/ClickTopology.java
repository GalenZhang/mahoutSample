package com.achievo.hadoop.storm.clicktopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.achievo.hadoop.storm.clicktopology.bolt.GeoStatsBolt;
import com.achievo.hadoop.storm.clicktopology.bolt.GeographyBolt;
import com.achievo.hadoop.storm.clicktopology.bolt.RepeatVisitBolt;
import com.achievo.hadoop.storm.clicktopology.bolt.VisitorStatsBolt;
import com.achievo.hadoop.storm.clicktopology.common.ConfKeys;
import com.achievo.hadoop.storm.clicktopology.common.FieldNames;
import com.achievo.hadoop.storm.clicktopology.common.HttpIpResolver;
import com.achievo.hadoop.storm.clicktopology.spout.ClickSpout;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ClickTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ClickTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ClickTopology
{
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("clickSpout", new ClickSpout(), 10);
		builder.setBolt("repeatBolt", new RepeatVisitBolt(), 10).shuffleGrouping("clickSpout");
		builder.setBolt("geographyBolt", new GeographyBolt(new HttpIpResolver()), 10).shuffleGrouping("clickSpout");
		builder.setBolt("totalStats", new VisitorStatsBolt(), 1).globalGrouping("repeatBolt");
		builder.setBolt("geoStats", new GeoStatsBolt(), 10).fieldsGrouping("geographyBolt",
			new Fields(FieldNames.COUNTRY));

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