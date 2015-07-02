package com.achievo.mahout.recommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.achievo.hadoop.hdfs.HdfsDAO;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ItemCFHadoop.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://github.com/bsspirit/maven_mahout_template/blob/mahout-0.8/src/main/java/org/conan/mymahout/recommendation/ItemCFHadoop.java
 * 
 *  Notes:
 * 	$Id: ItemCFHadoop.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 2, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ItemCFHadoop
{
	private static final String HDFS = "hdfs://localhost:9000";

	public static void main(String[] args) throws Exception
	{
		String localFile = ItemCFHadoop.class.getClassLoader().getResource("").getPath() + "datafile/item.csv";
		String inPath = HDFS + "/user/hadoop/userCF";
		String inFile = inPath + "/item.csv";
		String outPath = HDFS + "/user/hadoop/userCF/result/";
		String outFile = outPath + "/part-r-00000";
		String tmpPath = HDFS + "/tmp/" + System.currentTimeMillis();

		Configuration conf = config();
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.rmr(inPath);
		hdfs.mkdirs(inPath);
		hdfs.copyFile(localFile, inPath);
		hdfs.ls(inPath);
		hdfs.cat(inFile);

		StringBuilder sb = new StringBuilder();
		sb.append("--input ").append(inPath);
		sb.append(" --output ").append(outPath);
		sb.append(" --booleanData true");
		sb.append(" --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity");
		sb.append(" --tempDir ").append(tmpPath);
		args = sb.toString().split(" ");

		RecommenderJob job = new RecommenderJob();
		job.setConf(conf);
		job.run(args);

		hdfs.cat(outFile);
	}

	public static Configuration config()
	{
		Configuration conf = new Configuration();
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
}

/*
 * $Log: av-env.bat,v $
 */