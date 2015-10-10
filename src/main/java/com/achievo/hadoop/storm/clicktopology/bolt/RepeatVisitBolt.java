package com.achievo.hadoop.storm.clicktopology.bolt;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.clicktopology.common.ConfKeys;
import com.achievo.hadoop.storm.clicktopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: RepeatVisitBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: RepeatVisitBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class RepeatVisitBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 2226561353388908109L;

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
		port = Integer.valueOf(stormConf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis()
	{
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple input)
	{
		String clientKey = input.getStringByField(FieldNames.CLIENT_KEY);
		String url = input.getStringByField(FieldNames.URL);
		String key = url + ":" + clientKey;
		String value = jedis.get(key);

		if (value == null)
		{
			jedis.set(key, "visited");
			collector.emit(new Values(clientKey, url, Boolean.TRUE.toString()));
		}
		else
		{
			collector.emit(new Values(clientKey, url, Boolean.FALSE.toString()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.CLIENT_KEY, FieldNames.URL, FieldNames.UNIQUE));
	}

}

/*
 * $Log: av-env.bat,v $
 */