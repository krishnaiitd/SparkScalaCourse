package com.sundogsoftware.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmountSpentKrishna {

  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomer")

    val input = sc.textFile("data/customer-orders.csv")

    val mappedInput = input.map(parseLine)

//    mappedInput.take(5).foreach(println)

    val userExpend = mappedInput.reduceByKey((x, y) => x + y)

    // flip (userId, amount) -> (amount, userId)

    val sortedByAmount = userExpend.map(x => (x._2, x._1)).sortByKey().collect()

    sortedByAmount.foreach(println)
//    userExpend.take(5).foreach(println)
  }

}
