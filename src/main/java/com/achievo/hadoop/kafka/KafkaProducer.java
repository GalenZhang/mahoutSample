package com.achievo.hadoop.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: KafkaProducer.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: KafkaProducer.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class KafkaProducer extends Thread
{
	private final kafka.javaapi.producer.Producer<Integer, String> producer;

	private final String topic;

	private final Properties props = new Properties();

	public KafkaProducer(String topic)
	{
		this.topic = topic;
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "10.50.90.28:9092");
		producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
	}

	@Override
	public void run()
	{
		int messageNo = 1;
		long testTime = System.currentTimeMillis();
		while (true)
		{
			String messageStr = new String("Message_" + messageNo);
			System.out.println("Send: " + messageStr);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			messageNo++;
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