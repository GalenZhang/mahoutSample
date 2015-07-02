package com.achievo.mahout.recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Kmeans.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: Kmeans.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Kmeans
{
	public static void main(String[] args) throws IOException
	{
		List sampleData = MathUtil.readFileToVector("datafile/randomData.csv");

		int k = 3;
		double threshold = 0.01;

		List<Vector> randomPoints = MathUtil.chooseRandomPoints(sampleData, k);
		for (Vector vector : randomPoints)
		{
			System.out.println("Init Point center: " + vector);
		}

		List clusters = new ArrayList();
		for (int i = 0; i < k; i++)
		{
			clusters.add(new Kluster(randomPoints.get(i), i, new EuclideanDistanceMeasure()));
		}
		
//		List<List> finaClusters = KMeansDriver.run(input, clustersIn, output, convergenceDelta, maxIterations, runClustering, clusterClassificationThreshold, runSequential);
	}
}

/*
 * $Log: av-env.bat,v $
 */