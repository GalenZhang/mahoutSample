package com.achievo.hadoop.storm;

import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloWorldSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 414817003375604669L;

	private static final Log LOG = LogFactory.getLog(HelloWorldSpout.class);

	private SpoutOutputCollector collector;

	private int referenceNumber;

	private static final int MAX_NUMBER = 10;

	public HelloWorldSpout() {
		Random random = new Random();
		referenceNumber = random.nextInt(MAX_NUMBER);
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		Random random = new Random();
		int instanceNumber = random.nextInt(MAX_NUMBER);

		if (instanceNumber == referenceNumber) {
			collector.emit(new Values("Hello World!"));
		} else {
			collector.emit(new Values("Another message"));
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
