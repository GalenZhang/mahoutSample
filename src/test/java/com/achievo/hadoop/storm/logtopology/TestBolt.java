package com.achievo.hadoop.storm.logtopology;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.achievo.hadoop.storm.logtopology.common.Conf;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: TestBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: TestBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class TestBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -3956808755390649596L;

	private static final transient Logger LOG = LoggerFactory.getLogger(TestBolt.class);

	private static Jedis jedis;

	private String channel;

	public TestBolt(String channel)
	{
		this.channel = channel;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		jedis = new Jedis("localhost", Integer.parseInt(Conf.DEFAULT_JEDIS_PORT));
		jedis.connect();
	}

	@Override
	public void execute(Tuple input)
	{
		jedis.rpush(channel, input.getString(1));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{

	}

}

/*
 * $Log: av-env.bat,v $
 */