package com.achievo.mahout.recommendation.job;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.DataModelBuilder;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CityBlockSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.SpearmanCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: RecommendFactory.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://github.com/bsspirit/maven_mahout_template/blob/mahout-0.8/src/main/java/org/conan/mymahout/recommendation/job/RecommendFactory.java
 * 
 *  Notes:
 * 	$Id: RecommendFactory.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 2, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public final class RecommendFactory
{
	/**
	 * build Data model from file
	 * 
	 * @throws TasteException, IOException
	 */
	public static DataModel buildDataModel(String file) throws TasteException, IOException
	{
		return new FileDataModel(new File(file));
	}

	public static DataModel buildDataModelNoPref(String file) throws TasteException, IOException
	{
		return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(new FileDataModel(new File(file))));
	}

	public static DataModelBuilder buildDataModelNoPrefBuilder()
	{
		return new DataModelBuilder()
		{

			public DataModel buildDataModel(FastByIDMap<PreferenceArray> trainingData)
			{
				return new GenericBooleanPrefDataModel(GenericBooleanPrefDataModel.toDataMap(trainingData));
			}

		};
	}

	/**
	 * similarity
	 */
	public enum SIMILARITY
	{
		PEARSON, EUCLIDEAN, COSINE, TANIMOTO, LOGLIKELIHOOD, SPEARMAN, CITYBLOCK, FARTHEST_NEIGHBOR_CLUSTER, NEAREST_NEIGHBOR_CLUSTER
	}

	public static UserSimilarity userSimilarity(SIMILARITY type, DataModel m) throws TasteException
	{
		switch (type)
		{
			case PEARSON:
				return new PearsonCorrelationSimilarity(m);
			case COSINE:
				return new UncenteredCosineSimilarity(m);
			case TANIMOTO:
				return new TanimotoCoefficientSimilarity(m);
			case LOGLIKELIHOOD:
				return new LogLikelihoodSimilarity(m);
			case SPEARMAN:
				return new SpearmanCorrelationSimilarity(m);
			case CITYBLOCK:
				return new CityBlockSimilarity(m);
			case EUCLIDEAN:
			default:
				return new EuclideanDistanceSimilarity(m);
		}
	}

	public static ItemSimilarity itemSimilarity(SIMILARITY type, DataModel m) throws TasteException
	{
		switch (type)
		{
			case PEARSON:
				return new PearsonCorrelationSimilarity(m);
			case COSINE:
				return new UncenteredCosineSimilarity(m);
			case TANIMOTO:
				return new TanimotoCoefficientSimilarity(m);
			case LOGLIKELIHOOD:
				return new LogLikelihoodSimilarity(m);
			case CITYBLOCK:
				return new CityBlockSimilarity(m);
			case EUCLIDEAN:
			default:
				return new EuclideanDistanceSimilarity(m);
		}
	}

	// public static ClusterSimilarity clusterSimilarity(SIMILARITY type, UserSimilarity us)
	// {
	// switch (type)
	// {
	// case NEAREST_NEIGHBOR_CLUSTER:
	// return new NearestNeighborClusterSimilarity(us);
	// case FARTHEST_NEIGHBOR_CLUSTER:
	// default:
	// return new FarthestNeighborClusterSimilarity(us);
	// }
	// }

	public enum NEIGHBORHOOD
	{
		NEAREST, THRESHOLD
	}

	public static UserNeighborhood userNeighborhood(NEIGHBORHOOD type, UserSimilarity s, DataModel m, double num)
			throws TasteException
	{
		switch (type)
		{
			case NEAREST:
				return new NearestNUserNeighborhood((int) num, s, m);
			case THRESHOLD:
			default:
				return new ThresholdUserNeighborhood(num, s, m);
		}
	}

	public enum RECOMMENDER
	{
		USER, ITEM
	}

	public static RecommenderBuilder userRecommender(final UserSimilarity us, final UserNeighborhood un, boolean pref)
	{
		return pref ? new RecommenderBuilder()
		{

			public Recommender buildRecommender(DataModel dataModel) throws TasteException
			{
				return new GenericUserBasedRecommender(dataModel, un, us);
			}

		} : new RecommenderBuilder()
		{

			public Recommender buildRecommender(DataModel dataModel) throws TasteException
			{
				return new GenericBooleanPrefUserBasedRecommender(dataModel, un, us);
			}

		};
	}

	public static RecommenderBuilder itemRecommender(final ItemSimilarity is, boolean pref)
	{
		return pref ? new RecommenderBuilder()
		{

			public Recommender buildRecommender(DataModel dataModel) throws TasteException
			{
				return new GenericItemBasedRecommender(dataModel, is);
			}

		} : new RecommenderBuilder()
		{

			public Recommender buildRecommender(DataModel dataModel) throws TasteException
			{
				return new GenericBooleanPrefItemBasedRecommender(dataModel, is);
			}

		};
	}

	// public static RecommenderBuilder slopeOneRecommender() throws TasteException
	// {
	// return new RecommenderBuilder()
	// {
	// public Recommender buildRecommender(DataModel dataModel) throws TasteException
	// {
	// return new SlopeOneRecommender(dataModel);
	// }
	//
	// };
	// }
	//
	// public static RecommenderBuilder itemKNNRecommender(final ItemSimilarity is, final Optimizer op, final int n)
	// throws TasteException
	// {
	// return new RecommenderBuilder()
	// {
	// public Recommender buildRecommender(DataModel dataModel) throws TasteException
	// {
	// return new KnnItemBasedRecommender(dataModel, is, op, n);
	// }
	// };
	// }
	//
	// public static RecommenderBuilder svdRecommender(final Factorizer factorizer) throws TasteException
	// {
	// return new RecommenderBuilder()
	// {
	// public Recommender buildRecommender(DataModel dataModel) throws TasteException
	// {
	// return new SVDRecommender(dataModel, factorizer);
	// }
	// };
	// }
	//
	// public static RecommenderBuilder treeClusterRecommender(final ClusterSimilarity cs, final int n)
	// throws TasteException
	// {
	// return new RecommenderBuilder()
	// {
	// public Recommender buildRecommender(DataModel dataModel) throws TasteException
	// {
	// return new TreeClusteringRecommender(dataModel, cs, n);
	// }
	// };
	// }

	public static void showItems(long uid, List<RecommendedItem> recommendations, boolean skip)
	{
		if (!skip || recommendations.size() > 0)
		{
			System.out.printf("uid:%s,", uid);
			for (RecommendedItem recommendation : recommendations)
			{
				System.out.printf("(%s,%f)", recommendation.getItemID(), recommendation.getValue());
			}
			System.out.println();
		}
	}

	public enum EVALUATOR
	{
		AVERAGE_ABSOLUTE_DIFFERENCE, RMS
	}

	public static RecommenderEvaluator buildEvaluator(EVALUATOR type)
	{
		switch (type)
		{
			case RMS:
				return new RMSRecommenderEvaluator();
			case AVERAGE_ABSOLUTE_DIFFERENCE:
			default:
				return new AverageAbsoluteDifferenceRecommenderEvaluator();
		}
	}

	public static void evaluate(EVALUATOR type, RecommenderBuilder rb, DataModelBuilder mb, DataModel dm, double trainPt)
			throws TasteException
	{
		System.out.printf("%s Evaluster Score:%s\n", type.toString(),
			buildEvaluator(type).evaluate(rb, mb, dm, trainPt, 1.0));
	}

	public static void evaluate(RecommenderEvaluator re, RecommenderBuilder rb, DataModelBuilder mb, DataModel dm,
			double trainPt) throws TasteException
	{
		System.out.printf("Evaluater Source:%s\n", re.evaluate(rb, mb, dm, trainPt, 1.0));
	}

	/**
	 * statsEvaluator
	 */
	public static void statsEvaluator(RecommenderBuilder rb, DataModelBuilder mb, DataModel m, int topn)
			throws TasteException
	{
		RecommenderIRStatsEvaluator evaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = evaluator.evaluate(rb, mb, m, null, topn,
			GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		System.out.printf("Recommender IR Evaluator: [Precision:%s,Recall:%s]\n", stats.getPrecision(),
			stats.getRecall());
	}
}

/*
 * $Log: av-env.bat,v $
 */