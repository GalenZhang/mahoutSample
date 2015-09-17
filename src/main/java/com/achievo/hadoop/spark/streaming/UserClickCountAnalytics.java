package com.achievo.hadoop.spark.streaming;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;
import akka.japi.Option;
import akka.japi.Option.Some;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: UserClickCountAnalytics.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: UserClickCountAnalytics.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 15, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class UserClickCountAnalytics
{

	public static void main(String[] args)
	{
		String masterUrl = "local[1]";

		// Create a StreamingContext with the given master URL
		SparkConf conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat");
		JavaStreamingContext scc = new JavaStreamingContext(conf, new Duration(2000));

		// Kafka configurations
		Set<String> topics = new HashSet<String>();
		topics.add("user_events");
		String brokers = "localhost:9092";
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

		final int dbIndex = 1;
		final String clickHashKey = "app::users::click";

		// Create a direct stream
		JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(scc, String.class,
			String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaDStream<Some> events = kafkaStream.flatMap(new FlatMapFunction<Tuple2<String, String>, Some>()
		{

			@Override
			public Some call(Tuple2<String, String> truple2) throws Exception
			{
				JSONObject data = new JSONObject(truple2._2());
				return new Some(data);
			}
		});

		// Compute user click times
		JavaPairDStream<String, Integer> userClicks = events.mapToPair(new PairFunction<Option.Some, String, Integer>()
		{

			@Override
			public Tuple2<String, Integer> call(Some some) throws Exception
			{
				if (some.isDefined())
				{
					JSONObject event = (JSONObject) some.get();
					return new Tuple2<String, Integer>(event.getString("uid"), event.getInt("click_count"));
				}
				return null;
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>()
		{

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception
			{
				return i1 + i2;
			}
		});

		userClicks.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>()
		{

			@Override
			public Void call(JavaPairRDD<String, Integer> rdd) throws Exception
			{
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>()
				{

					@Override
					public void call(Iterator<Tuple2<String, Integer>> partitionOfRecords) throws Exception
					{
						while (partitionOfRecords.hasNext())
						{
							Tuple2<String, Integer> pair = partitionOfRecords.next();
							String uid = pair._1();
							int clickCount = pair._2();
							Jedis jedis = RedisClient.pool.getResource();
							jedis.select(dbIndex);
							jedis.hincrBy(clickHashKey, uid, clickCount);
							RedisClient.pool.close();
						}

					}
				});
				return null;
			}

		});
		
		scc.start();
		scc.awaitTermination();
	}

	private static class RedisClient
	{
		static String redisHost = "localhost";

		static int redisPort = 6379;

		static int redisTimeout = 30000;

		public static JedisPool pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort, redisTimeout);

	}

}

/*
 * $Log: av-env.bat,v $
 */