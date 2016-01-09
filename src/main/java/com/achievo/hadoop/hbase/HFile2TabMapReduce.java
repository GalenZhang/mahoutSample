package com.achievo.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class HFile2TabMapReduce extends Configured implements Tool {

	public static class HFile2TabMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
						throws IOException, InterruptedException {
			String[] words = value.toString().split("\t");
			// rk0001 zhangsan 33

			Put put = new Put(Bytes.toBytes(words[0]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(words[1]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(words[2]));
			rowkey.set(Bytes.toBytes(words[0]));

			context.write(rowkey, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// create job
		Job job = Job.getInstance(getConf(), this.getClass().getSimpleName());

		// set run jar class
		job.setJarByClass(this.getClass());

		// set input . output
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// set map
		job.setMapperClass(HFile2TabMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// set reduce
		job.setReducerClass(PutSortReducer.class);

		// set hfile output
		Connection conn = ConnectionFactory.createConnection(getConf());
		TableName tn = TableName.valueOf(args[0]);
		Table table = conn.getTable(tn);
		Admin admin = conn.getAdmin();
		RegionLocator regionLocator = conn.getRegionLocator(tn);
		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

		// submit job
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException(" error with job !!!");
		}

		// load hfile
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
		loader.doBulkLoad(new Path(args[2]), admin, table, regionLocator);

		admin.close();
		conn.close();

		return 0;
	}

}
