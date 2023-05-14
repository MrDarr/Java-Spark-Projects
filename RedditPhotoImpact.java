package com.RUSpark;

/* any necessary Java packages here */
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RedditPhotoImpact {
	public static final String punctuation = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		//String InputPath = args[0];
		
		/* Implement Here */ 
		
		SparkSession spark = SparkSession.builder()
		        .appName("RedditPhotoImpact")
		        .getOrCreate();

		JavaRDD<String> lines = spark.sparkContext()
		        .textFile(args[0], 1)
		        .toJavaRDD();

		JavaPairRDD<Integer, Integer> indiv = lines
		        .mapToPair(s -> {
		            String[] fields = s.split(punctuation);
		            int key = Integer.parseInt(fields[0]);
		            int value = Integer.parseInt(fields[4]) + Integer.parseInt(fields[5]) + Integer.parseInt(fields[6]);
		            return new Tuple2<>(key, value);
		        });

		JavaPairRDD<Integer, Integer> counts = indiv.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<Integer, Integer>> output = counts.collect();
		
		for (Tuple2<Integer, Integer> tuple : output) {
			System.out.println(tuple._1() + " " + tuple._2());
		}
		spark.stop();
	}

}
