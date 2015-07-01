package com.achievo.mahout.recommendation;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: UserCF.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.fens.me/hadoop-mahout-maven-eclipse/
 * 
 *  Notes:
 * 	$Id: UserCF.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 1, 2015		galen.zhang		Initial.
 *  
 * </pre>
 */
public class UserCF
{
	final static int NEIGHBORHOOD_NUM = 2;
	
	final static int RECOMMENDER_NUM = 3;
	
	public static void main(String[] args) throws IOException, TasteException
	{
		String file = UserCF.class.getClassLoader().getResource("").getPath() + "datafile/item.csv";
		DataModel model = new FileDataModel(new File(file));
		UserSimilarity user = new EuclideanDistanceSimilarity(model);
		NearestNUserNeighborhood neighbor = new NearestNUserNeighborhood(NEIGHBORHOOD_NUM, user, model);
		Recommender r = new GenericUserBasedRecommender(model, neighbor, user);
		LongPrimitiveIterator iter = model.getUserIDs();
		
		while (iter.hasNext())
		{
			long uid = iter.nextLong();
			List<RecommendedItem> list = r.recommend(uid, RECOMMENDER_NUM);
			System.out.printf("uid:%s", uid);
			for (RecommendedItem ritem : list)
			{
				System.out.printf("(%s,%f)", ritem.getItemID(), ritem.getValue());
			}
			System.out.println();
		}
	}
}

/*
*$Log: av-env.bat,v $
*/