package com.achievo.hadoop.kafka;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: KafkaConsumerProducerDemo.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: KafkaConsumerProducerDemo.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class KafkaConsumerProducerDemo
{

	public static void main(String[] args)
	{
		KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
		producerThread.start();

		KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
		consumerThread.start();
	}

}

/*
 * $Log: av-env.bat,v $
 */