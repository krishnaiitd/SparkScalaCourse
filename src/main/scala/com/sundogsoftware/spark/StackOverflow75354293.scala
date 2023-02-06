package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StructType}

/** Compute the average number of friends by age in a social network. */
object StackOverflow75354293 {

  case class Game(player_no: Int, points: Float)

  /** A function that splits a line of input into (player_no, points) tuples. */
  def parseLine(line: String): (Int, Float) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the player_no and points fields, and convert to integer & float
    val player_no = fields(0).toInt
    val points = fields(1).toFloat
    // Create a tuple that is our result.
    (player_no, points)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "StackOverflow75354293")

    val spark = SparkSession.builder
      .appName("PopularGame")
      .master("local[*]")
      .getOrCreate()

    val scheam = new StructType()
      .add("player_no", IntegerType, nullable = true)
      .add("points", FloatType, nullable = true)

    // Load up movie data as dataset
    import spark.implicits._
    val given_dataset = spark.read
      .option("sep", ",")
      .schema(scheam)
      .csv("data/stackoverflowdata-noheader.csv")
      .as[Game]

    // Use our parseLines function to convert to (player_no, points) tuples
    val total_points_dataset = given_dataset.groupBy("player_no").sum("points").orderBy("player_no")
    val games_played_dataset = given_dataset.groupBy($"player_no").count().orderBy("player_no")
    val avg_points_dataset = given_dataset.groupBy($"player_no").avg("points").orderBy("player_no")
    total_points_dataset.show()
    games_played_dataset.show()
    avg_points_dataset.show()

    val lines = sc.textFile("data/stackoverflowdata-noheader.csv")
    val dataset = lines.map(parseLine)
    val total_points_dataset2 = dataset.reduceByKey((x, y) => x + y)
    val total_points_dataset2_sorted = total_points_dataset2.sortByKey(ascending = true)
    total_points_dataset2_sorted.foreach(println)
    val games_played_dataset2 = dataset.countByKey().toList.sorted
    games_played_dataset2.foreach(println)
    val avg_points_dataset2 =
      dataset
        .mapValues(x => (x, 1))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .mapValues(x => x._1 / x._2)
        .sortByKey(ascending = true)
    avg_points_dataset2.collect().foreach(println)
    // Lots going on here...
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    // We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    // Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
    // adding together all the numFriends values and 1's respectively.
//    val totalsByAgePair = rdd.mapValues(x => (x, 1))
////    totalsByAgePair.take(5).foreach(println)
//
//    val totalsByAge = totalsByAgePair.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
////    totalsByAge.take(5).foreach(println)
//    // So now we have tuples of (age, (totalFriends, totalInstances))
//    // To compute the average we divide totalFriends / totalInstances for each age.
//    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
//
//    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
//    val results = averagesByAge.collect()
//
//    // Sort and print the final results.
//    results.sorted.take(5).foreach(println)
  }

}
