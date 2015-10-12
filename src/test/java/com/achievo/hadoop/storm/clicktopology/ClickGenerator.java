package com.achievo.hadoop.storm.clicktopology;

import java.util.UUID;

import org.json.simple.JSONObject;
import org.junit.Test;

import com.achievo.hadoop.storm.clicktopology.common.FieldNames;

import redis.clients.jedis.Jedis;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ClickGenerator.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ClickGenerator.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ClickGenerator
{
	private static final String REDIS_HOST = "localhost";

	private static final int REDIS_PORT = 6379;

	private static final int CLIENT_COUNT = 8;

	private static final String[] ips = new String[] {"180.149.132.47", "125.39.240.113", "121.194.0.221",
			"61.145.122.155", "104.130.219.184", "11.75.115.70", "205.204.96.36", "182.22.39.242",};

	private static final String[] urls = new String[] {"baidu.com", "qq.com", "weibo.com", "oschina.net",
			"storm.apache.org", "taobao.com", "alibaba.com", "yahoo.co.jp"};

	private Jedis jedis;

	@Test
	public void test()
	{
		connectToRedis();
		pushTuples();
		disconnectFromRedis();
	}

	private void disconnectFromRedis()
	{
		System.out.println("Disconnect from Redis server.");
		jedis.disconnect();
	}

	@SuppressWarnings("unchecked")
	private void pushTuples()
	{
		System.out.println("Push click tuples: ");
		for (int i = 0; i < CLIENT_COUNT; i++)
		{
			JSONObject clickTuple = new JSONObject();
			clickTuple.put(FieldNames.IP, ips[i]);
			clickTuple.put(FieldNames.URL, urls[i]);
			clickTuple.put(FieldNames.CLIENT_KEY, UUID.randomUUID().toString());

			String jsonText = clickTuple.toJSONString();
			System.out.println("    Push: " + jsonText);
			jedis.rpush("count", jsonText);
		}
	}

	private void connectToRedis()
	{
		System.out.println("Connect to Redis server: ");
		System.out.println("    host: " + REDIS_HOST);
		System.out.println("    port: " + REDIS_PORT);

		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
		jedis.connect();
	}
}

/*
 * $Log: av-env.bat,v $
 */