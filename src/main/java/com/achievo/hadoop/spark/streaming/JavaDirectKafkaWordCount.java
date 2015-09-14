package com.achievo.hadoop.spark.streaming;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.storm.guava.collect.Lists;

import scala.Tuple2;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: JavaDirectKafkaWordCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: JavaDirectKafkaWordCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class JavaDirectKafkaWordCount
{

	public static void main(String[] args)
	{
		String brokers = "localhost:9092";
		String topic = "my-replicated-topic5";

		// Create context with 2 second batch interval
		SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		HashSet<String> topicsSet = new HashSet<String>();
		topicsSet.add(topic);
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
			StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String,String>, String>()
		{

			@Override
			public String call(Tuple2<String, String> truple2) throws Exception
			{
				return truple2._2();
			}
		});
		
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{

			@Override
			public Iterable<String> call(String str) throws Exception
			{
				return Lists.newArrayList(str.split("\\s+"));
			}
		});
	}

}

/*
 * $Log: av-env.bat,v $
 */