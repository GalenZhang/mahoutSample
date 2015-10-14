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
 *  File: SupportComputeBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SupportComputeBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SupportComputeBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 1098924257689591323L;

	private OutputCollector collector;

	private Map<ItemPair, Integer> pairCounts;

	int pairTotalCount;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		pairCounts = new HashMap<ItemPair, Integer>();
		pairTotalCount = 0;
	}

	@Override
	public void execute(Tuple input)
	{
		if (input.getFields().get(0).equals(FieldNames.TOTAL_COUNT))
		{
			pairTotalCount = input.getIntegerByField(FieldNames.TOTAL_COUNT);
		}
		else if (input.getFields().size() == 3)
		{
			String item1 = input.getStringByField(FieldNames.ITEM1);
			String item2 = input.getStringByField(FieldNames.ITEM2);
			int pairCount = input.getIntegerByField(FieldNames.PAIR_COUNT);
			pairCounts.put(new ItemPair(item1, item2), pairCount);
		}
		else if (input.getFields().get(0).equals(FieldNames.COMMAND))
		{
			for (ItemPair itemPair : pairCounts.keySet())
			{
				double itemSupport = (double) (pairCounts.get(itemPair).intValue()) / pairTotalCount;
				collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2, FieldNames.SUPPORT));
	}

}

/*
 * $Log: av-env.bat,v $
 */