package com.achievo.hadoop.spark.job;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 *  File: IPAddressStats.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: IPAddressStats.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class IPAddressStats implements Serializable
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 665578313294758186L;

	private static final Log LOG = LogFactory.getLog(IPAddressStats.class);

	public void stat(String[] args)
	{
		SparkConf conf = new SparkConf().setAppName("IPAddressStats").setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(conf);

		JavaRDD<String> lines = ctx.textFile("hdfs://localhost:9000/user/hadoop/people.txt", 1);

		// splits and extracts ip address filed
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>()
		{
			@Override
			public Iterable<String> call(String s) throws Exception
			{
				// 121.205.198.92 - - [21/Feb/2014:00:00:07 +0800] "GET /archives/417.html HTTP/1.1" 200 11465
				// "http://shiyanjun.cn/archives/417.html/"
				// "Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0"
				return Arrays.asList(s.split("\\s+")[0]);
			}
		});
		// map
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>()
		{
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception
			{
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		// reduce
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			@Override
			public Integer call(Integer i1, Integer i2) throws Exception
			{
				return i1 + i2;
			}
		});

		// sort statistics result by value
		List<Tuple2<String, Integer>> output = counts.collect();
		Collections.sort(output, new Comparator<Tuple2<String, Integer>>()
		{
			@Override
			public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2)
			{
				if (t1._2() < t2._2())
				{
					return 1;
				}
				else if (t1._2() > t2._2())
				{
					return -1;
				}
				return 0;
			}

		});
	}
}

/*
 * $Log: av-env.bat,v $
 */