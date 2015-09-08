package com.achievo.hadoop.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: KafkaConsumer.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: KafkaConsumer.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class KafkaConsumer extends Thread
{
	private final ConsumerConnector consumer;

	private final String topic;

	public KafkaConsumer(String topic)
	{
		this.topic = topic;
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
	}

	private static ConsumerConfig createConsumerConfig()
	{
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaProperties.zkConnect);
		props.put("group.id", KafkaProperties.groupId);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	@Override
	public void run()
	{
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> iter = stream.iterator();
		long testTime = System.currentTimeMillis();

		while (iter.hasNext())
		{
			System.out.println("receive: " + new String(iter.next().message()));
			try
			{
				sleep(3000);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			if (System.currentTimeMillis() - testTime > 20000)
			{
				break;
			}
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */