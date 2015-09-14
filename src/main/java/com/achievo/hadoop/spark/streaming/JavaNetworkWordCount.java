package com.achievo.hadoop.spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SparkStreamingTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SparkStreamingTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class JavaNetworkWordCount
{

	public static void main(String[] args)
	{
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{
			@Override
			public Iterable<String> call(String str) throws Exception
			{
				return Arrays.asList(str.split("\\s+"));
			}
		});

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(String word) throws Exception
			{
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
			@Override
			public Integer call(Integer i1, Integer i2) throws Exception
			{
				return i1 + i2;
			}
			
		});
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();
		
		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate
		// nc -lk 9999
		// $ ./bin/run-example streaming.JavaNetworkWordCount localhost 9999
	}

}

/*
 * $Log: av-env.bat,v $
 */