package com.achievo.hadoop.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ConsumerTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ConsumerTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ConsumerTest implements Runnable
{
	private KafkaStream<byte[], byte[]> stream;

	private int threadNumber;

	public ConsumerTest(KafkaStream<byte[], byte[]> stream, int threadNumber)
	{
		this.stream = stream;
		this.threadNumber = threadNumber;
	}

	@Override
	public void run()
	{
		ConsumerIterator<byte[], byte[]> iter = stream.iterator();
		while (iter.hasNext())
		{
			System.out.println("Thread " + threadNumber + " : " + new String(iter.next().message()));
		}
		System.out.println("Shutting down Thread: " + threadNumber);
	}

}

/*
 * $Log: av-env.bat,v $
 */