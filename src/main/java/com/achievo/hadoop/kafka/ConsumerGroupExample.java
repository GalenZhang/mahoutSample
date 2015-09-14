package com.achievo.hadoop.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ConsumerGroupExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ConsumerGroupExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ConsumerGroupExample
{
	private final ConsumerConnector consumer;

	private final String topic;

	private ExecutorService executor;

	public ConsumerGroupExample(String zks, String groupId, String topic)
	{
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zks, groupId));
		this.topic = topic;
	}

	private ConsumerConfig createConsumerConfig(String zks, String groupId)
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", zks);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit,interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public void shutdown()
	{
		if (consumer != null)
		{
			consumer.shutdown();
		}
		if (executor != null)
		{
			executor.shutdown();
		}

		try
		{
			if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
			{
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		}
		catch (InterruptedException e)
		{
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int numThread)
	{
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(numThread));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		executor = Executors.newFixedThreadPool(numThread);
		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams)
		{
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}
	}
	
	public static void main(String[] args)
	{
		String zks = "localhost:2181";
		String groupId = "test";
		String topic = "test";
		int threads = 5;
		
		ConsumerGroupExample example = new ConsumerGroupExample(zks, groupId, topic);
		example.run(threads);
		
		try
		{
			Thread.sleep(10000);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		example.shutdown();
	}
}

/*
 * $Log: av-env.bat,v $
 */