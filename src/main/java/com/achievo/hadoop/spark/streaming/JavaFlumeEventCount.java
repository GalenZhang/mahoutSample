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
 *  File: JavaFlumeEventCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: JavaFlumeEventCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 *  
 * </pre>
 */
public class JavaFlumeEventCount
{

	public static void main(String[] args)
	{
//		flume configure
//		agent.sinks = avroSink
//		agent.sinks.avroSink.type = avro
//		agent.sinks.avroSink.channel = memoryChannel
//		agent.sinks.avroSink.hostname = <chosen machine's hostname>
//		agent.sinks.avroSink.port = <chosen port on the machine>
		
		SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(ssc, "localhost", 9999);
		
		flumeStream.count();
		
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
*$Log: av-env.bat,v $
*/