package com.achievo.hadoop.storm.logtopology.bolt;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.logtopology.common.FieldNames;
import com.achievo.hadoop.storm.logtopology.model.LogEntry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: VolumeCountingBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: VolumeCountingBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class VolumeCountingBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -1815267555668656987L;

	public static Logger LOG = LoggerFactory.getLogger(VolumeCountingBolt.class);

	private OutputCollector collector;

	public static final String FIELD_ROW_KEY = "RowKey";

	public static final String FIELD_COLUMN = "Column";

	public static final String FIELD_INCREMENT = "IncrementAmount";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input)
	{
		LogEntry entry = (LogEntry) input.getValueByField(FieldNames.LOG_ENTRY);
		collector.emit(new Values(getMinuteForTime(entry.getTimestamp()), entry.getSource(), 1L));
	}

	private Object getMinuteForTime(Date timestamp)
	{
		Calendar c = Calendar.getInstance();
		c.setTime(timestamp);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);

		return c.getTimeInMillis();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FIELD_ROW_KEY, FIELD_COLUMN, FIELD_INCREMENT));
	}

}

/*
 * $Log: av-env.bat,v $
 */