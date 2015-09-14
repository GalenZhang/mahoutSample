package com.achievo.hadoop.spark.streaming;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.storm.guava.collect.Lists;

import scala.Tuple2;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: JavaKafkaWordCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: JavaKafkaWordCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class JavaKafkaWordCount
{

	public static void main(String[] args)
	{
		String zks = "localhost:2181";
		String topic = "my-replicated-topic5";

		SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
		// Create the context with a 1 second batch size
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put(topic, 5);

		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc, zks, "group1",	topicMap);
		JavaDStream<String> lines = kafkaStream.map(new Function<Tuple2<String, String>, String>()
		{
			@Override
			public String call(Tuple2<String, String> tuple2) throws Exception
			{
				return tuple2._2();
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

		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>()
		{

			@Override
			public Tuple2<String, Integer> call(String s) throws Exception
			{
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>()
		{

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception
			{
				return i1 + i2;
			}
		});

		wordCounts.print();
		jssc.start();
		jssc.awaitTermination();
	}

}

/*
 * $Log: av-env.bat,v $
 */