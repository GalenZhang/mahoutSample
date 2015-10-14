package com.achievo.hadoop.storm.logtopology.spout;

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

import com.achievo.hadoop.storm.logtopology.common.Conf;
import com.achievo.hadoop.storm.logtopology.common.FieldNames;
import com.achievo.hadoop.storm.logtopology.model.LogEntry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: LogSpout.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: LogSpout.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class LogSpout extends BaseRichSpout
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 5452353968512225035L;

	public static Logger LOG = LoggerFactory.getLogger(LogSpout.class);

	public static final String LOG_CHANNEL = "log";

	private Jedis jedis;

	private String host;

	private int port;

	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		this.collector = collector;
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.parseInt(conf.get(Conf.REDIS_PORT_KEY).toString());
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
		String content = jedis.rpop(LOG_CHANNEL);

		if (content == null || "nil".equals(content))
		{
			try
			{
				Thread.sleep(300);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			JSONObject obj = (JSONObject) JSONValue.parse(content);
			LogEntry entry = new LogEntry(obj);
			collector.emit(new Values(entry));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.LOG_ENTRY));
	}

}

/*
 * $Log: av-env.bat,v $
 */