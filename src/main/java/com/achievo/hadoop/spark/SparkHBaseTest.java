package com.achievo.hadoop.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SparkHBaseTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SparkHBaseTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 24, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SparkHBaseTest
{

	public static void main(String[] args)
	{
		SparkConf sparkConf = new SparkConf().setAppName("hbaseTest").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		Configuration conf = HBaseConfiguration.create();
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf"));
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("airName"));

		try
		{
			String tableName = "flight_wap_order_log";
			conf.set(TableInputFormat.INPUT_TABLE, tableName);
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			String scanToString = Base64.encodeBytes(proto.toByteArray());
			conf.set(TableInputFormat.SCAN, scanToString);
			JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);

			System.out.println(myRDD.count());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			sc.close();
		}
	}

}

/*
 * $Log: av-env.bat,v $
 */