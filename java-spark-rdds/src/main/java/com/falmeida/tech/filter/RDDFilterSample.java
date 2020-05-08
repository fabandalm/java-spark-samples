package com.falmeida.tech.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFilterSample {

	public static void main(String[] args) {
		
		List<String> inputData = new ArrayList<String>();
		inputData.add("WARN: 2016-12-31 04:19:32");
		inputData.add("FATAL: 2016-12-31 03:22:34");
		inputData.add("WARN: 2016-12-31 03:21:21");
		inputData.add("INFO: 2015-4-21 14:32:21");
		inputData.add("FATAL: 2015-4-21 19:23:20");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.parallelize(inputData);
		
 		JavaRDD<String> words = lines.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		JavaRDD<String> filtered = words.filter(word -> word.length() > 4);
 		
		filtered.collect().forEach(System.out::println);
 		
		sc.close();
	}
	
}
