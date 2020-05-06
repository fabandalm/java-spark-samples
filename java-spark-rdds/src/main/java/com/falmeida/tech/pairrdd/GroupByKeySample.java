package com.falmeida.tech.pairrdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class GroupByKeySample {
		
	public static void main(String[] args) {
		
		List<String> inputData = new ArrayList<String>();
		inputData.add("WARN:2016-12-31 04:19:32");
		inputData.add("FATAL:2016-12-31 03:22:34");
		inputData.add("WARN:2016-12-31 03:21:21");
		inputData.add("INFO:2015-4-21 14:32:21");
		inputData.add("FATAL:2015-4-21 19:23:20");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		JavaRDD<String> rddSample = sc.parallelize(inputData);
		
		JavaPairRDD<String,Long> pairRDD = rddSample.mapToPair(rawValue -> {
			
			String[] columns = rawValue.split(":");
			String level = columns[0];
			
			return new Tuple2<String,Long>(level,1L);
		});
		
		JavaPairRDD<String,Long> sumsPairRDD = pairRDD.reduceByKey((value1,value2) -> value1 + value2);
		
		sumsPairRDD.foreach(tuple -> System.out.println(tuple._1 +" has " + tuple._2 + " instances"));
		*/
		
		// Group By Key is not recommended for performance impact.
		
		sc.parallelize(inputData)
			.mapToPair( rawValue -> new Tuple2<>(rawValue.split(":")[0],1L))
			.groupByKey()
			.foreach(tuple -> System.out.println(tuple._1 +" has " + Iterables.size(tuple._2) + " instances"));
		
		sc.close();
		
	}
	
}
