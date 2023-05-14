package com.RUSpark;

/* any necessary Java packages here */
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
public class NetflixMovieAverage {
	public static final String punctuation = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
 // Read the input path from the command line arguments
    String inputPath = args[0];

    // Create a Spark session
    SparkSession spark = SparkSession.builder()
        .appName("NetflixMovieAverage")
        .getOrCreate();

    // Read the lines of the input file as a JavaRDD
    JavaRDD<String> lines = spark.read()
        .textFile(inputPath)
        .javaRDD();

    // Map each line to a (movieId, (rating, 1)) tuple
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> ratings = lines
        .mapToPair(line -> {
            String[] fields = line.split(punctuation);
            int movieId = Integer.parseInt(fields[0]);
            int rating = Integer.parseInt(fields[2]);
            return new Tuple2<>(movieId, new Tuple2<>(rating, 1));
        });

    // Compute the total rating and count for each movie
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> totals = ratings
        .reduceByKey((t1, t2) -> new Tuple2<>(t1._1() + t2._1(), t1._2() + t2._2()));

    // Compute the average rating for each movie and print the results
    List<Tuple2<Integer, Double>> averages = totals
        .mapValues(t -> 1.0 * t._1() / t._2())
        .collect();

    for (Tuple2<Integer, Double> average : averages) {
        System.out.printf("%d %.2f\n", average._1(), average._2());
    }

    // Stop the Spark session
    spark.stop();

		
	}

}
