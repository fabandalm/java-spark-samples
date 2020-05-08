package com.falmeida.tech.reader;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDReadFromDiskSample {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("spark-sample").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("src/main/resources/subtitles/input.txt");
		
 		JavaRDD<String> words = lines.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		words.collect().forEach(System.out::println);
 		
		sc.close();
	}
	
}
