package com.achievo.hadoop.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: FlumePollingEventCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: FlumePollingEventCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class FlumePollingEventCount
{

	public static void main(String[] args)
	{
//		flume configure
//		agent.sinks = spark
//		agent.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
//		agent.sinks.spark.hostname = <hostname of the local machine>
//		agent.sinks.spark.port = <port to listen on for connection from Spark>
//		agent.sinks.spark.channel = memoryChannel
		
		// Create the context and set the batch size
		SparkConf sparkConf = new SparkConf().setAppName("FlumePollingEventCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(2000));

		// Create a flume stream that polls the Spark Sink running in a Flume agent
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(ssc, "localhost", 9999);
		flumeStream.count().map(new Function<Long, String>()
		{

			@Override
			public String call(Long in) throws Exception
			{
				return "Received " + in + " flume events.";
			}
		});
		
		ssc.start();
		ssc.awaitTermination();
	}

}

/*
 * $Log: av-env.bat,v $
 */