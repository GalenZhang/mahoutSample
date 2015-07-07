package com.achievo.mahout.recommendation.book;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: BookFilterGenderResult.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: BookFilterGenderResult.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 7, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class BookFilterGenderResult
{
	final static int NEIGHBORHOOD_NUM = 2;

	final static int RECOMMENDER_NUM = 3;

	public static void main(String[] args) throws TasteException, IOException
	{
		String file = BookFilterGenderResult.class.getClassLoader().getResource("").getPath()
				+ "datafile/book/rating.csv";
		DataModel dataModel = RecommendFactory.buildDataModel(file);
		RecommenderBuilder rb1 = BookEvaluator.userEuclidean(dataModel);
		RecommenderBuilder rb2 = BookEvaluator.itemEuclidean(dataModel);
		RecommenderBuilder rb3 = BookEvaluator.userEuclideanNoPref(dataModel);
		RecommenderBuilder rb4 = BookEvaluator.itemEuclideanNoPref(dataModel);

		long uid = 65;
		System.out.print("userEuclidean       =>");
		filterGender(uid, rb1, dataModel);
		System.out.print("itemEuclidean       =>");
		filterGender(uid, rb2, dataModel);
		System.out.print("userEuclideanNoPref =>");
		filterGender(uid, rb3, dataModel);
		System.out.print("itemEuclideanNoPref =>");
		filterGender(uid, rb4, dataModel);
	}

	/**
	 * 对用户性别进行过滤
	 * 
	 * @throws IOException
	 * @throws TasteException
	 */
	public static void filterGender(long uid, RecommenderBuilder recommenderBuilder, DataModel dataModel)
			throws IOException, TasteException
	{
		Set<Long> userids = getMale(BookFilterGenderResult.class.getClassLoader().getResource("").getPath()
				+ "datafile/book/user.csv");

		// 计算男性用户打分过的图书
		Set<Long> bookids = new HashSet<Long>();
		for (Long uids : userids)
		{
			LongPrimitiveIterator iter = dataModel.getItemIDsFromUser(uids).iterator();
			while (iter.hasNext())
			{
				long bookid = iter.next();
				bookids.add(bookid);
			}
		}

		IDRescorer rescorer = new FilterRescorer(bookids);
		List<RecommendedItem> list = recommenderBuilder.buildRecommender(dataModel).recommend(uid, RECOMMENDER_NUM,
			rescorer);
		RecommendFactory.showItems(uid, list, false);
	}

	/**
	 * 获得男性用户ID
	 * 
	 * @throws IOException
	 */
	public static Set<Long> getMale(String file) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(new File(file)));
		Set<Long> userids = new HashSet<Long>();
		String s = null;
		while ((s = br.readLine()) != null)
		{
			String[] cols = s.split(",");
			if (cols[1].equals("M")) // 判断男性用户
			{
				userids.add(Long.parseLong(cols[0]));
			}
		}
		br.close();
		return userids;
	}

}

/**
 * 对结果重计算
 */
class FilterRescorer implements IDRescorer
{
	private final Set<Long> userids;

	public FilterRescorer(Set<Long> userids)
	{
		this.userids = userids;
	}

	public double rescore(long id, double originalScore)
	{
		return isFiltered(id) ? Double.NaN : originalScore;
	}

	public boolean isFiltered(long id)
	{
		return userids.contains(id);
	}

}

/*
 * $Log: av-env.bat,v $
 */