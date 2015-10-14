package com.achievo.hadoop.storm.ordertopology.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.ordertopology.common.FieldNames;
import com.achievo.hadoop.storm.ordertopology.common.ItemPair;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: PairCountBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: PairCountBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class PairCountBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -5172850448291091161L;

	private OutputCollector collector;

	private Map<ItemPair, Integer> pairCounts;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		pairCounts = new HashMap<ItemPair, Integer>();
	}

	@Override
	public void execute(Tuple input)
	{
		String item1 = input.getStringByField(FieldNames.ITEM1);
		String item2 = input.getStringByField(FieldNames.ITEM2);
		ItemPair itemPair = new ItemPair(item1, item2);
		int pairCount = 0;

		if (pairCounts.containsKey(itemPair))
		{
			pairCount = pairCounts.get(itemPair);
		}
		pairCount++;
		pairCounts.put(itemPair, pairCount);

		collector.emit(new Values(item1, item2, pairCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.PAIR_COUNT));
	}

}

/*
 * $Log: av-env.bat,v $
 */