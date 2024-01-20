package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.time.{Instant, LocalDate}
import java.time.temporal.TemporalAdjusters
import scala.concurrent.duration.Duration
import java.nio.charset.Charset
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit

object HelloWorld {

  def schema(fields: Any*): StructType = StructType(fields map {
    case field: StructField => field
    case (name: String, dataType: DataType) => StructField(name, dataType)
    case (name: String, dataType: String) => StructField(name, DataType.fromDDL(dataType))
    case unsupported => throw new IllegalArgumentException(s"Unsupported field $unsupported")
  })

  def dataframe(fields: Any*)(data: Product*): DataFrame = dataframe(schema(fields: _*))(data: _*)

  def main(args: Array[String]): Unit = {

    //    val sc = new SparkContext("local[*]", "HelloWorld")

    val conf = new SparkConf().setMaster("local[*]").setAppName("HelloWorld").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    //    val lines = sc.textFile("data/ml-100k/u.data")
    //    val numLines = lines.count()

    ////    println("Hello world! The u.data file has " + numLines + " lines.")
    //
    //    println("Hello World " + sc.getAllPools.toList.toString())
    ////    val df = dataframe( ("id", DataType., "name")()

    val value = "LATEST";

    if (value != "TRIM_HORIZON" && value != "LATEST")
      println("Yes")
    else
      println("No")
//    val hudiTimestampFormat = new SimpleDateFormat("YYYYMMddHHmmssSSS")
//    val tt = hudiTimestampFormat.format("2018-12-03 00:50:00.000")
//    println(tt)
//    //////
//
//    val batchFrom = "2018-12-03 00:50:00.000"
//    val optFrom =
//      try Option(Timestamp.valueOf(batchFrom))
//
//    val maxBatchInterval = Duration.apply("1day")
//    val extractPartitionFormat = Some("yyyy-MM")
//    //      catch throw new Exception(s"Please provide the valid batch from values e.g ")
//    var from = optFrom.get
//
//    println(from)
//
//    //    val aa = from.toLocalDateTime.`with`(TemporalAdjusters.firstDayOfMonth())
//    //
//    ////    val localDate = LocalDate.of(aa.get.getYear, from.getMonth, from.getDay)
//    //    val firstDayOfMonth = aa.`with`(TemporalAdjusters.firstDayOfMonth())
//    //    val firstDayOfYear = aa.`with`(TemporalAdjusters.firstDayOfYear())
//    //    val firstDay = aa.withHour(0).withMinute(0).withSecond(0).withNano(0)
//    //
//    //    println(firstDayOfMonth)
//    //    println(firstDayOfYear)
//    //    println(firstDay)
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    //    from = getStartOfValue(from, extractPartitionFormat.get, maxBatchInterval)
//
//    print("From is: ")
//    println(from)
//
//    val toString = "2019-12-08 00:50:00.000"
//    val to = Option(Timestamp.valueOf(toString))
//    println("To is: " + to)
//
//    val partition = getPartitionValue(from, extractPartitionFormat.get)
//    //
//    println("Partition is: ")
//    println(partition)
//
//    val partitions = getPartitionValues(from, to.get, extractPartitionFormat.get)
//    println("partitions " + partitions)
    sc.stop()
  }

  def getPartitionValues(from: Timestamp, to: Timestamp, extractPartitionFormat: String): List[String] = {
    val fromLocalDate = from.toLocalDateTime.toLocalDate
    val toLocalDate = to.toLocalDateTime.toLocalDate

    val partitionValues = extractPartitionFormat match {
      case "yyyy-MM-dd" => ChronoUnit.DAYS.between(fromLocalDate, toLocalDate)
      case "yyyy-MM" => ChronoUnit.MONTHS.between(fromLocalDate, toLocalDate)
      case "yyyy" => ChronoUnit.YEARS.between(fromLocalDate, toLocalDate)
      case _ => throw JobFatalError(s"Please provide extract partition as yyyy-MM-dd/yyyy-MM/yyyy only")
    }

    val partitionValuesList = (0 to partitionValues.toInt).map { i =>
      extractPartitionFormat match {
        case "yyyy-MM-dd" => fromLocalDate.plusDays(i)
        case "yyyy-MM" => fromLocalDate.plusMonths(i)
        case "yyyy" => fromLocalDate.plusYears(i)
        case _ => throw JobFatalError(s"Please provide extract partition as yyyy-MM-dd/yyyy-MM/yyyy only")
      }
    }

    partitionValuesList.map(from => getPartitionValue(from.toString, extractPartitionFormat)).toList
  }

  def getPartitionValue(from: String, extractPartitionFormat: String): String = {
    println(from)
    println(extractPartitionFormat)
    val a = Timestamp.valueOf(from)
    extractPartitionFormat match {
      case "yyyy-MM-dd" => new SimpleDateFormat("yyyy-MM-dd").format(a)
      case "yyyy-MM" => new SimpleDateFormat("yyyy-MM").format(a)
      case "yyyy" => new SimpleDateFormat("yyyy").format(a)
      case _ => throw JobFatalError(s"Please provide extract partition as yyyy-MM-dd/yyyy-MM/yyyy only")
    }
  }

  def getPartitionValue(from: Timestamp, extractPartitionFormat: String): String = extractPartitionFormat match {
    case "yyyy-MM-dd" => new SimpleDateFormat("yyyy-MM-dd").format(from)
    case "yyyy-MM" => new SimpleDateFormat("yyyy-MM").format(from)
    case "yyyy" => new SimpleDateFormat("yyyy").format(from)
    case _ => throw JobFatalError(s"Please provide extract partition as yyyy-MM-dd/yyyy-MM/yyyy only")
  }

//  def getStartOfValue(from: Timestamp, extractPartitionFormat: String, maxBatchInterval: Duration): Timestamp = {
//    val aa = from.toLocalDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0)
//    val baseFrom = extractPartitionFormat match {
//      case "yyyy-MM-dd" => aa
//      case "yyyy-MM" => aa.`with`(TemporalAdjusters.firstDayOfMonth())
//      case "yyyy" => aa.`with`(TemporalAdjusters.firstDayOfYear())
//      case _ => throw JobFatalError(s"Please provide extract partition as yyyy-MM-dd/yyyy-MM/yyyy only")
//    }
//    val nextTime = Timestamp.valueOf(baseFrom)
//
//    val maxBatchInterval = Duration.apply("1day")
//
//    nextTime + maxBatchInterval
//  }

  implicit class TimeUnitMixin(unit: TimeUnit) {

    def toChronoUnit: ChronoUnit = unit match {
      case TimeUnit.DAYS => ChronoUnit.DAYS
      case TimeUnit.HOURS => ChronoUnit.HOURS
      case TimeUnit.MINUTES => ChronoUnit.MINUTES
      case TimeUnit.SECONDS => ChronoUnit.SECONDS
      case TimeUnit.MILLISECONDS => ChronoUnit.MILLIS
      case TimeUnit.MICROSECONDS => ChronoUnit.MICROS
      case TimeUnit.NANOSECONDS => ChronoUnit.NANOS
    }
  }

//  implicit class InstantMixin(self: Instant) {
//    def +(duration: Duration): Instant = self.plus(duration.length, duration.unit.toChronoUnit)
//
//    def -(duration: Duration): Instant = self.minus(duration.length, duration.unit.toChronoUnit)
//  }
//
//  implicit class TimestampMixin(self: Timestamp) {
//    def +(duration: Duration): Timestamp = Timestamp.from(self.toInstant + duration)
//
//    def -(duration: Duration): Timestamp = Timestamp.from(self.toInstant - duration)
//  }
}

// partion -> 2019-02

//from : 2019-02-01, 2019-02-05
//  ..2019-02-05, 2019-02-10

//run 10
//2019-02-25, 2019-03-02

// 2019-03-02.before(2019-03-05)
// change of partition -> 2019-03-01 00:00.000
// -> 2019-03-05

//2019-03-05.before(2019-03-05)
//

//from: 2019-03-05 and 2019-03-10
//..
//..
// 2019-03-30 -> 2019-04-04

//run change of partition
// partition: 2019-04
// from >
// 2019-04-04.before(2019-04-05) -> true => startFromTimestamp => 2019-04-01 00:00:00.0000
// from -> 2019-04-01 00:00:00.0000
