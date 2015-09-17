package com.achievo.hadoop.spark.streaming;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: KafkaEventProducer.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: KafkaEventProducer.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class KafkaEventProducer
{
	private static String[] users = {"4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
			"011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf", "068b746ed4620d25e26055a9f804385f",
			"97edfc08311c70143401745a03a50706", "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
			"6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"};

	private static Random random = new Random();

	private static int pointer = -1;

	public static String getUserID()
	{
		pointer++;
		if (pointer >= users.length)
		{
			pointer = 0;
		}
		return users[pointer];
	}

	public static int click()
	{
		return random.nextInt(10);
	}

	// bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events
	// --replication-factor 2 --partitions 2
	// bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
	// bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
	// bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event
	// --from-beginning

	public static void main(String[] args) throws JSONException, InterruptedException
	{
		String topic = "user_events";
		String brokers = "localhost:9092";
		Properties props = new Properties();
		props.put("metamata.broker.list", brokers);

		ProducerConfig kafkaConfig = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(kafkaConfig);

		while (true)
		{
			// prepare event data
			JSONObject event = new JSONObject();
			event.put("uid", getUserID()).put("event_time", System.currentTimeMillis()).put("os_type", "Android")
					.put("click_count", click());
			
			// produce event message
			producer.send(new KeyedMessage<String, String>(topic, event.toString()));
			System.out.println("Message sent: " + event);
			
			Thread.sleep(200);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */