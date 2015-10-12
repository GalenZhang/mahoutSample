package com.achievo.hadoop.storm.ordertopology.common;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ItemPair.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ItemPair.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ItemPair
{
	private String item1;

	private String item2;

	public ItemPair(String item1, String item2)
	{
		this.item1 = item1;
		this.item2 = item2;
	}

	public String getItem1()
	{
		return item1;
	}

	public String getItem2()
	{
		return item2;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}

		if (this == obj)
		{
			return true;
		}

		if (obj instanceof ItemPair == false)
		{
			return false;
		}

		ItemPair itemPair = (ItemPair) obj;
		if (this.item1 != null && this.item1.equals(itemPair.item1) && this.item2 != null
				&& this.item2.equals(itemPair.item2))
		{
			return true;
		}

		return false;
	}

	@Override
	public int hashCode()
	{
		int h1 = 0;
		int h2 = 0;
		if (item1 != null)
		{
			h1 = item1.hashCode();
		}
		if (item2 != null)
		{
			h2 = item2.hashCode();
		}

		return h1 + h2;
	}

	@Override
	public String toString()
	{
		return item1 + ":" + item2;
	}
}

/*
 * $Log: av-env.bat,v $
 */