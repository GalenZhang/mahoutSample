package com.achievo.hadoop.storm.ordertopology.bolt;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.ordertopology.common.ConfKeys;
import com.achievo.hadoop.storm.ordertopology.common.FieldNames;
import com.achievo.hadoop.storm.ordertopology.common.ItemPair;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ConfidenceComputeBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ConfidenceComputeBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ConfidenceComputeBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 3500711151106434226L;

	private OutputCollector collector;

	private Jedis jedis;

	private String host;

	private int port;

	private Map<ItemPair, Integer> pairCounts;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		host = stormConf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.parseInt(stormConf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();

		pairCounts = new HashMap<ItemPair, Integer>();
	}

	private void connectToRedis()
	{
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple input)
	{
		if (input.getFields().size() == 3)
		{
			String item1 = input.getStringByField(FieldNames.ITEM1);
			String item2 = input.getStringByField(FieldNames.ITEM2);
			int pairCount = input.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		}
		else if (input.getFields().get(0).equals(FieldNames.COMMAND))
		{
			for (ItemPair itemPair : pairCounts.keySet())
			{
				int item1Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem1()));
				int item2Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem2()));
				double pairCount = pairCounts.get(itemPair).intValue();
				double itemConfidence = 0.0d;
				if (item1Count < item2Count)
				{
					itemConfidence = pairCount / item1Count;
				}
				else
				{
					itemConfidence = pairCount / item2Count;
				}

				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemConfidence));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.CONFIDENCE));
	}

}

/*
 * $Log: av-env.bat,v $
 */