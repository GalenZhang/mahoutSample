package com.achievo.hadoop.storm.trident.tfidf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.achievo.hadoop.storm.trident.tfidf.common.Conf;
import com.achievo.hadoop.storm.trident.tfidf.functions.DocumentFetchFunction;
import com.achievo.hadoop.storm.trident.tfidf.functions.DocumentTokenizer;
import com.achievo.hadoop.storm.trident.tfidf.functions.TermFilter;
import com.achievo.hadoop.storm.trident.tfidf.functions.TfidfExpression;
import com.hmsonline.storm.cassandra.trident.CassandraMapState;
import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: TermTopology.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TermTopology.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 19, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TermTopology
{
	@SuppressWarnings("unused")
	private static Logger log = LoggerFactory.getLogger(TermTopology.class);

	private static StateFactory getStateFactory(String rowKey)
	{
		Options options = new Options<Object>();
		options.columnFamily = "tfidf";
		// options.
		return CassandraMapState.nonTransactional(options);
	}

	@SuppressWarnings({"unused", "unchecked", "rawtypes"})
	public static TridentTopology buildTopology(ITridentSpout spout, LocalDRPC drpc)
	{
		TridentTopology topology = new TridentTopology();

		FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("url"), 1, new Values(
				"/home/stormdev/Document/article/1.txt"), new Values("/home/stormdev/Document/article/2.txt"));
		testSpout.setCycle(true);

		Stream documentStream = topology.newStream("doucmentSpout", testSpout).parallelismHint(20)
				.each(new Fields("url"), new DocumentFetchFunction(), new Fields("document", "documentId", "source"));

		Stream termStream = documentStream.parallelismHint(20)
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("dirtyTerm"))
				.each(new Fields("dirtyTerm"), new TermFilter(), new Fields("term"))
				.project(new Fields("term", "documentId", "source"));

		TridentState dfState = termStream.groupBy(new Fields("term")).persistentAggregate(getStateFactory("df"),
			new Count(), new Fields("df"));

		TridentState dState = termStream.groupBy(new Fields("source")).persistentAggregate(getStateFactory("d"),
			new Count(), new Fields("d"));

		topology.newDRPCStream("dQuery", drpc).each(new Fields("args"), new Split(), new Fields("source"))
				.stateQuery(dState, new Fields("source"), new MapGet(), new Fields("d_term", "currentD"));

		topology.newDRPCStream("dfQuery", drpc).each(new Fields("args"), new Split(), new Fields("term"))
				.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("currentDf"));

		Stream tfidfStream = termStream.groupBy(new Fields("documentId", "term"))
				.aggregate(new Count(), new Fields("tf"))
				.each(new Fields("term", "documentId", "tf"), new TfidfExpression(), new Fields("tfidf"));
		return topology;
	}

	public static void main(String args[]) throws Exception
	{
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(Conf.REDIS_PORT_KEY, Conf.REDIS_DEFAULT_JEDIS_PORT);
		conf.put("DOCUMENT_PATH", "document.avro");

		if (args.length == 0)
		{
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			TridentTopology topology = buildTopology(null, drpc);

			cluster.submitTopology("tfidf", conf, topology.build());
			Utils.sleep(60000);
			cluster.killTopology("tfidf");
			cluster.shutdown();
		}
		else
		{
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null, null).build());
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */