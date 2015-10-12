package com.achievo.hadoop.storm.ordertopology;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.achievo.hadoop.storm.ordertopology.common.FieldNames;

import redis.clients.jedis.Jedis;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: OrderGenerator.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: OrderGenerator.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class OrderGenerator
{
	private static final String REDIS_HOST = "localhost";

	private static final int REDIS_PORT = 6379;

	private static final int ORDER_COUNT = 30;

	private Jedis jedis;

	private Random random = new Random(1000);

	private static final String[] ITEMS_NAME = new String[] {"milk", "coffee", "egg", "flower", "icecream", "wine",
			"water", "fish", "golf", "CD", "beer"};

	@Before
	public void setUp()
	{
		connectToRedis();
	}

	@After
	public void clean()
	{
		disconnectFromRedis();
	}

	@Test
	public void test()
	{
		pushTuples();
	}

	@Test
	public void testPop()
	{
		popTuple();
	}

	private void popTuple()
	{
		String content = jedis.rpop("order");
		JSONObject obj = (JSONObject) JSONValue.parse(content);
		String id = obj.get(FieldNames.ID).toString();
		JSONArray items = (JSONArray) obj.get(FieldNames.ITEMS);
		System.out.println(id);
		for (Object itemObj : items)
		{
			JSONObject item = (JSONObject) itemObj;
			String name = item.get(FieldNames.NAME).toString();

			int count = Integer.parseInt(item.get(FieldNames.COUNT).toString());
			System.out.println(name + ":" + count);
		}
	}

	@SuppressWarnings("unchecked")
	private void pushTuples()
	{
		System.out.println("Push order tuples: ");
		for (int i = 0; i < ORDER_COUNT; i++)
		{
			JSONObject orderTuple = new JSONObject();

			JSONArray items = new JSONArray();
			Set<String> selectedItems = new HashSet<String>();

			for (int j = 0; j < 4; j++)
			{
				JSONObject item = new JSONObject();

				while (true)
				{
					int itemIndex = random.nextInt(ITEMS_NAME.length);
					String itemName = ITEMS_NAME[itemIndex];

					if (!selectedItems.contains(itemName))
					{
						item.put(FieldNames.NAME, itemName);
						item.put(FieldNames.COUNT, random.nextInt(100));
						items.add(item);
						selectedItems.add(itemName);
						break;
					}
				}
			}
			orderTuple.put(FieldNames.ID, UUID.randomUUID().toString());
			orderTuple.put(FieldNames.ITEMS, items);

			String jsonText = orderTuple.toJSONString();
			System.out.println(jsonText);
			jedis.rpush("orders", jsonText);
		}
	}

	private void disconnectFromRedis()
	{
		System.out.println("Disconnect from Redis server.");
		jedis.disconnect();
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