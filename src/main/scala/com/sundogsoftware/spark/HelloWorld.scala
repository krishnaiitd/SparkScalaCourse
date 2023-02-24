package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.lang
import java.sql.Timestamp
import java.lang.RuntimeException

object HelloWorld {

  def schema(fields: Any*): StructType = StructType(fields map {
    case field: StructField => field
    case (name: String, dataType: DataType) => StructField(name, dataType)
    case (name: String, dataType: String) => StructField(name, DataType.fromDDL(dataType))
    case unsupported => throw new IllegalArgumentException(s"Unsupported field $unsupported")
  })

  def dataframe(fields: Any*)(data: Product*): DataFrame = dataframe(schema(fields: _*))(data: _*)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val lines = sc.textFile("data/ml-100k/u.data")
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

//    val df = dataframe( ("id", DataType., "name")()
////
//
//    val batchFrom = "2018-09-01 00:00:00.001"
//    val optFrom =
//      try Option(Timestamp.valueOf(batchFrom))
////      catch throw new Exception(s"Please provide the valid batch from values e.g ")
//
//    println(optFrom.get.getTime)

    sc.stop()
  }
}
