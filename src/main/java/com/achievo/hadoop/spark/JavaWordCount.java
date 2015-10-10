package com.achievo.hadoop.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: JavaWordCount.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaWordCount.java
 * 
 *  Notes:
 * 	$Id: JavaWordCount.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 30, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class JavaWordCount
{

	@SuppressWarnings("serial")
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(args[0], 1);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{
			@Override
			public Iterable<String> call(String t) throws Exception
			{
				return Arrays.asList(t.split("\\s+"));
			}
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception
			{
				return new Tuple2<String, Integer>(t, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception
			{
				return v1 + v2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple : output)
		{
			System.out.println(tuple._1() + " : " + tuple._2());
		}

		ctx.stop();
		ctx.close();
	}

}

/*
 * $Log: av-env.bat,v $
 */