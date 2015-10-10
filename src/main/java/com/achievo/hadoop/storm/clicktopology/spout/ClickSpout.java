package com.achievo.hadoop.storm.clicktopology.spout;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.clicktopology.common.ConfKeys;
import com.achievo.hadoop.storm.clicktopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ClickSpout.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ClickSpout.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ClickSpout extends BaseRichSpout
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -7008170145273732089L;

	private static Logger LOG = LoggerFactory.getLogger(ClickSpout.class);

	private Jedis jedis;

	private String host;

	private int port;

	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		this.collector = collector;

		host = conf.get(ConfKeys.REDIS_HOST).toString();
		port = Integer.valueOf(conf.get(ConfKeys.REDIS_PORT).toString());
		connectToRedis();
	}

	private void connectToRedis()
	{
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void nextTuple()
	{
		String content = jedis.rpop("count");

		if (content == null || "nil".equals(content))
		{
			try
			{
				Thread.sleep(300);
			}
			catch (InterruptedException e)
			{
				LOG.info(e.getMessage());
				e.printStackTrace();
			}
		}
		else
		{
			JSONObject obj = (JSONObject) JSONValue.parse(content);
			String ip = obj.get(FieldNames.IP).toString();
			String url = obj.get(FieldNames.URL).toString();
			String clientKey = obj.get(FieldNames.CLIENT_KEY).toString();

			collector.emit(new Values(ip, url, clientKey));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.IP, FieldNames.URL, FieldNames.CLIENT_KEY));
	}

}

/*
 * $Log: av-env.bat,v $
 */