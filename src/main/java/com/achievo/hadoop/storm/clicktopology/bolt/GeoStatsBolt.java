package com.achievo.hadoop.storm.clicktopology.bolt;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.clicktopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: GeoStatsBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: GeoStatsBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class GeoStatsBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -7654479870147327285L;

	private class CountryStats
	{
		private int countryTotal = 0;

		private static final int COUNT_INDEX = 0;

		private static final int PERCENTAGE_INDEX = 1;

		private String countryName;

		// List index 0 count total, 1 percentage
		private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();

		public CountryStats(String countryName)
		{
			this.countryName = countryName;
		}

		public void cityFound(String cityName)
		{
			countryTotal++;
			if (cityStats.containsKey(cityName))
			{
				cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX).intValue() + 1);
			}
			else
			{
				// List index 0 count total, 1 percentage
				List<Integer> list = new LinkedList<Integer>();
				list.add(1);
				list.add(0);
				cityStats.put(cityName, list);
			}

			double percent = (double) cityStats.get(cityName).get(COUNT_INDEX).intValue() / countryTotal;
			cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
		}

		public int getCountryTotal()
		{
			return countryTotal;
		}

		public int getCityTotal(String cityName)
		{
			return cityStats.get(cityName).get(COUNT_INDEX).intValue();
		}

		public String toString()
		{
			return "Total count for " + countryName + " is " + Integer.toString(countryTotal) + "\nCities: "
					+ cityStats.toString();
		}
	}

	private OutputCollector collector;

	private Map<String, CountryStats> stats = new HashMap<String, CountryStats>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		String country = input.getStringByField(FieldNames.COUNTRY);
		String city = input.getStringByField(FieldNames.CITY);

		if (!stats.containsKey(country))
		{
			stats.put(country, new CountryStats(country));
		}
		stats.get(country).cityFound(city);
		collector.emit(new Values(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(
			city)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.COUNTRY, FieldNames.COUNTRY_TOTAL, FieldNames.CITY,
				FieldNames.CITY_TOTAL));
	}

}

/*
 * $Log: av-env.bat,v $
 */