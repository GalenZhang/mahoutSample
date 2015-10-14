package com.achievo.hadoop.storm.ordertopology.bolt;

import java.util.Map;

import org.json.simple.JSONObject;

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
 *  File: FilterBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: FilterBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class FilterBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 7181380418595237140L;

	private static final double SUPPORT_THRESHOLD = 0.01;

	private static final double CONFIDENCE_THRESHOLD = 0.01;

	private OutputCollector collector;

	private Jedis jedis;

	private String host;

	private int port;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		host = stormConf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.parseInt(stormConf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis()
	{
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input)
	{
		String item1 = input.getStringByField(FieldNames.ITEM1);
		String item2 = input.getStringByField(FieldNames.ITEM2);
		ItemPair pair = new ItemPair(item1, item2);
		String pairString = pair.toString();

		double support = 0;
		double confidence = 0;

		if (input.getFields().get(2).equals(FieldNames.SUPPORT))
		{
			support = input.getDoubleByField(FieldNames.SUPPORT);
			jedis.hset("supports", pairString, String.valueOf(support));
		}
		else if (input.getFields().get(2).equals(FieldNames.CONFIDENCE))
		{
			confidence = input.getDoubleByField(FieldNames.CONFIDENCE);
			jedis.hset("confidences", pairString, String.valueOf(confidence));
		}

		if (!jedis.hexists("supports", pairString) || !jedis.hexists("confidences", pairString))
		{
			return;
		}

		support = Double.parseDouble(jedis.hget("supports", pairString));
		confidence = Double.parseDouble(jedis.hget("confidences", pairString));

		if (support >= SUPPORT_THRESHOLD && confidence >= CONFIDENCE_THRESHOLD)
		{
			JSONObject pairValue = new JSONObject();
			pairValue.put(FieldNames.SUPPORT, support);
			pairValue.put(FieldNames.CONFIDENCE, confidence);
			jedis.hset("recommendedPairs", pair.toString(), pairValue.toJSONString());

			collector.emit(new Values(item1, item2, support, confidence));
		}
		else
		{
			jedis.hdel("recommendedPairs", pair.toString());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.SUPPORT, FieldNames.CONFIDENCE));
	}

}

/*
 * $Log: av-env.bat,v $
 */