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

//    OUT PUT Of above code in my local system
    //    +---------+-----------+
    //|player_no|sum(points)|
    //+---------+-----------+
    //|        1|       66.0|
    //|        2|       33.0|
    //|        3|       78.0|
    //+---------+-----------+
    //
    //+---------+-----+
    //|player_no|count|
    //+---------+-----+
    //|        1|    3|
    //|        2|    2|
    //|        3|    3|
    //+---------+-----+
    //
    //+---------+-----------+
    //|player_no|avg(points)|
    //+---------+-----------+
    //|        1|       22.0|
    //|        2|       16.5|
    //|        3|       26.0|
    //+---------+-----------+
    //
    //(1,66.0)
    //(2,33.0)
    //(3,78.0)
    //(1,3)
    //(2,2)
    //(3,3)
    //(1,22.0)
    //(2,16.5)
    //(3,26.0)
    //
    //Process finished with exit code 0
  }

}
