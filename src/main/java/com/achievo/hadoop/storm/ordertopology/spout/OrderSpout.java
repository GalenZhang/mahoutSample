package com.achievo.hadoop.storm.ordertopology.spout;

import java.util.Map;

import org.json.simple.JSONArray;
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

import com.achievo.hadoop.storm.ordertopology.common.ConfKeys;
import com.achievo.hadoop.storm.ordertopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: OrderSpout.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: OrderSpout.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class OrderSpout extends BaseRichSpout
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 4202465092414548982L;

	private static Logger LOG = LoggerFactory.getLogger(OrderSpout.class);

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
		port = Integer.parseInt(conf.get(ConfKeys.REDIS_PORT).toString());
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
		String content = jedis.rpop("orders");

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
			String id = obj.get(FieldNames.ID).toString();
			JSONArray items = (JSONArray) obj.get(FieldNames.ITEMS);
			System.out.println(id);
			for (Object itemObj : items)
			{
				JSONObject item = (JSONObject) itemObj;
				String name = item.get(FieldNames.NAME).toString();
				int count = Integer.parseInt(item.get(FieldNames.COUNT).toString());
				collector.emit(new Values(id, name, count));

				if (jedis.hexists("itemCount", name))
				{
					jedis.hincrBy("itemCount", name, 1);
				}
				else
				{
					jedis.hset("itemCount", name, "1");
				}
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ID, FieldNames.NAME, FieldNames.COUNT));
	}

}

/*
 * $Log: av-env.bat,v $
 */