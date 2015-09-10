package com.achievo.hadoop.storm;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class HelloWorldBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4200884413706717436L;

	private static final Log LOG = LogFactory.getLog(HelloWorldBolt.class);

	private int myCount = 0;

	@Override
	public void execute(Tuple input) {
		String test = input.getStringByField("sentence");
		if (test.equals("Hello World!")) {
			myCount++;
			System.out.println("Found a Hello World!");
			LOG.info("Found a Hello World!");
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("myCount"));
	}

	@Override
	public void cleanup() {
		LOG.info("Result count: " + myCount);
	}

}
