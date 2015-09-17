package com.achievo.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.Tuple3;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SparkOnHBase.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://gist.github.com/wuchong/95630f80966d07d7453b#file-sparkonhbase-scala
 * 
 *  Notes:
 * 	$Id: SparkOnHBase.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 17, 2015		galen.zhang		Initial.
 *  
 * </pre>
 */
/**
 * Spark 读取和写入 HBase
 **/
public class SparkOnHBase
{
	public static String convertScanToString(Scan scan) throws IOException
	{
		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		return Base64.encodeBytes(proto.toByteArray());
	}

	// step 2: rdd mapping to table

	// 在 HBase 中表的 schema 一般是这样的
	// *row cf:col_1 cf:col_2
	// 而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14) , (2,"hanmei",18)
	// 我们需要将 *RDD[(uid:Int, name:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
	// 我们定义了 convert 函数做这个转换工作
	public static Tuple2<ImmutableBytesWritable, Put> convert(Tuple3<Integer, String, Integer> tuple)
	{
		Put p = new Put(Bytes.toBytes(tuple._1()));
		p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("name"), Bytes.toBytes(tuple._2()));
		p.addColumn(Bytes.toBytes("basic"), Bytes.toBytes("age"), Bytes.toBytes(tuple._3()));
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), p);
	}

	public static void main(String[] args) throws IOException
	{
		JavaSparkContext sc = new JavaSparkContext("local", "SparkOnHBase");

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "2181");

		// ======Save RDD to HBase========
		// step 1: JobConf setup
		JobConf jobConf = new JobConf(conf, SparkOnHBase.class);
		jobConf.setOutputFormat(TableOutputFormat.class);

		// step 3: read RDD data from somewhere and convert
		List<Tuple3<Integer, String, Integer>> rawData = new ArrayList<Tuple3<Integer, String, Integer>>();
		rawData.add(new Tuple3<Integer, String, Integer>(1, "lilei", 14));
		rawData.add(new Tuple3<Integer, String, Integer>(2, "hanmei", 18));
		rawData.add(new Tuple3<Integer, String, Integer>(3, "someone", 38));

		JavaPairRDD<ImmutableBytesWritable, Put> localData = sc.parallelize(rawData).mapToPair(
			new PairFunction<Tuple3<Integer, String, Integer>, ImmutableBytesWritable, Put>()
			{

				@Override
				public Tuple2<ImmutableBytesWritable, Put> call(Tuple3<Integer, String, Integer> tuple3)
						throws Exception
				{
					return convert(tuple3);
				}
			});

		// step 4: use "saveAsHadoopDataset" to save RDD to HBase
		localData.saveAsHadoopDataset(jobConf);
		// =================================

		// ======Load RDD from HBase========
		// use `newAPIHadoopRDD` to load RDD from HBase
		// 直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

		// 设置查询的表名
		conf.set(TableInputFormat.INPUT_TABLE, "user");

		// 添加过滤条件，年龄大于 18 岁
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter("basic".getBytes(), "age".getBytes(), CompareOp.GREATER_OR_EQUAL,
				Bytes.toBytes(18)));
		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		JavaPairRDD<ImmutableBytesWritable, Result> usersRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
			ImmutableBytesWritable.class, Result.class);

		long count = usersRDD.count();
		System.out.println("Users RDD Count: " + count);
		usersRDD.cache();

		// 遍历输出
		usersRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>()
		{
			@Override
			public void call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception
			{
				Result result = tuple._2();
				int key = Bytes.toInt(result.getRow());
				String name = Bytes.toString(result.getValue("basic".getBytes(), "name".getBytes()));
				int age = Bytes.toInt(result.getValue("basic".getBytes(), "age".getBytes()));
				System.out.println("Row key: " + key + " Name: " + name + " Age: " + age);
			}
		});

		sc.close();
		// =================================
	}

}

/*
 * $Log: av-env.bat,v $
 */