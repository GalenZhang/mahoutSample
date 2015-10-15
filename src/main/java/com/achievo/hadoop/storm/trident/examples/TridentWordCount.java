package com.achievo.hadoop.storm.trident.examples;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: TridentWordCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TridentWordCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TridentWordCount
{
	public static class Split extends BaseFunction
	{
		/**
		 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
		 */
		private static final long serialVersionUID = -4448893385104673521L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector)
		{
			String sentence = tuple.getString(0);
			for (String word : sentence.split("\\s+"))
			{
				collector.emit(new Values(word));
			}
		}
	}

	@SuppressWarnings("unchecked")
	public static StormTopology buildTopology(LocalDRPC drpc)
	{
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values(
				"the cow jumped over the moon"), new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"), new Values("how many apples can you eat"), new Values(
						"to be or not to be the person"));
		spout.setCycle(true);

		TridentTopology topology = new TridentTopology();

		TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16)
				.each(new Fields("sentence"), new Split(), new Fields("word")).groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
				.parallelismHint(16);

		topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word"))
				.groupBy(new Fields("word")).stateQuery(wordCounts, new MapGet(), new Fields("count"))
				.each(new Fields("count"), new FilterNull())
				.aggregate(new Fields("count"), new Sum(), new Fields("sum"));

		return topology.build();
	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException
	{
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0)
		{
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
			for (int i = 0; i < 100; i++)
			{
				System.out.println("DRPC result: " + drpc.execute("words", "cat the dog jumped"));
				Utils.sleep(1000);
			}
			cluster.killTopology("wordCounter");
			cluster.shutdown();
		}
		else
		{
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
		}

	}

}

/*
 * $Log: av-env.bat,v $
 */