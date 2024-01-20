package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.sql.Timestamp
import java.time.{Instant, LocalDate, YearMonth}
import scala.concurrent.duration.{Duration, DurationInt}
import java.lang
import java.sql.Timestamp
import java.lang.RuntimeException
import java.time.format.DateTimeFormatter
import scala.concurrent.duration
import scala.concurrent.duration.{Duration, DurationInt}

abstract class JobError(message: String, cause: Option[Throwable]) extends RuntimeException(message, cause.orNull)

class JobFatalError(message: String, cause: Option[Throwable]) extends JobError(message, cause)

object JobFatalError {
  def apply(message: String): JobFatalError = apply(message, None)
  def apply(message: String, cause: Throwable): JobFatalError = apply(message, Some(cause))
  def apply(message: String, cause: Option[Throwable]): JobFatalError = new JobFatalError(message, cause)
}

object HelloWorldModified {

  def schema(fields: Any*): StructType = StructType(fields map {
    case field: StructField => field
    case (name: String, dataType: DataType) => StructField(name, dataType)
    case (name: String, dataType: String) => StructField(name, DataType.fromDDL(dataType))
    case unsupported => throw new IllegalArgumentException(s"Unsupported field $unsupported")
  })

  def dataframe(fields: Any*)(data: Product*): DataFrame = dataframe(schema(fields: _*))(data: _*)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    // Create a SparkSession using every core of the local machine
    val spark = SparkSession.builder
      .appName("Testing")
//      .master("local[*]")
      .getOrCreate()

    val patternLayout = DateTimeFormatter.ofPattern("M/d/uuuu")

    val batchFrom = "2019-01-01 00:00:00.001"
    val from = Timestamp.valueOf(batchFrom)

//    val to = from + Duration.apply("10h")

//    println(to)

    val resource = "merchant-transaction-2019-02"
    val endOfMonth = getEndOfMonths(resource)
    println(from)
    println(endOfMonth)
    if (from.after(endOfMonth))
      println("End of month reached")
    else
      println("Still in the month")
    spark.stop()
  }

  def getEndOfMonths(resource: String): Timestamp = {
    val patternLayout = DateTimeFormatter.ofPattern("M/d/uuuu")
    val givenYear = resource.drop("merchant-transaction-".length).take(4)
    val givenMonth = resource.drop("merchant-transaction-".length).drop(5)
    val monthDateYearFormat =
      givenMonth + "/01/" + givenYear

    val ld = LocalDate.parse(monthDateYearFormat, patternLayout)
    val yearMonth = YearMonth.from(ld)
    val endOfMonth = yearMonth.atEndOfMonth

    Timestamp.valueOf(endOfMonth + " 23:59:59.999")
  }
}
