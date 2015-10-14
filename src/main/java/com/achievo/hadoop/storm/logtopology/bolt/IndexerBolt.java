package com.achievo.hadoop.storm.logtopology.bolt;

import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.logtopology.common.Conf;
import com.achievo.hadoop.storm.logtopology.common.FieldNames;
import com.achievo.hadoop.storm.logtopology.model.LogEntry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: IndexerBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: IndexerBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class IndexerBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -2077258693649682591L;

	public static Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

	private OutputCollector collector;

	private Client client;

	public static final String INDEX_NAME = "logstorm";

	public static final String INDEX_TYPE = "logentry";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		Node node;
		if ((Boolean) stormConf.get(backtype.storm.Config.TOPOLOGY_DEBUG) == true)
		{
			node = NodeBuilder.nodeBuilder().local(true).node();
		}
		else
		{
			String clusterName = (String) stormConf.get(Conf.ELASTIC_CLUSTER_NAME);
			if (clusterName == null)
			{
				clusterName = Conf.DEFAULT_ELASTIC_CLUSTER;
			}
			node = NodeBuilder.nodeBuilder().clusterName(clusterName).node();
		}

		client = node.client();
	}

	@Override
	public void execute(Tuple input)
	{
		LogEntry entry = (LogEntry) input.getValueByField(FieldNames.LOG_ENTRY);
		if (entry == null)
		{
			LOG.error("Received null or incorrect value from tuple");
			return;
		}

		String toBeIndexed = entry.toJSON();
		IndexResponse response = client.prepareIndex(INDEX_NAME, INDEX_TYPE).setSource(toBeIndexed).execute()
				.actionGet();
		if (response == null)
		{
			LOG.error("Failed to index tuple: " + input.toString());
		}
		else
		{
			if (response.getId() == null)
			{
				LOG.error("Failed to index tuple: " + input.toString());
			}
			else
			{
				LOG.debug("Indexing success on tuple: " + input.toString());
				collector.emit(new Values(entry, response.getId()));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.LOG_ENTRY, FieldNames.LOG_INDEX_ID));
	}

}

/*
 * $Log: av-env.bat,v $
 */