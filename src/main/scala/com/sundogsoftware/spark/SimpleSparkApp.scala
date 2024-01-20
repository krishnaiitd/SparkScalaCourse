package com.sundogsoftware.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

// spark-shell --master yarn   --name spark-job-shell-kp   --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:2.4.4,org.apache.avro:avro:1.8.2,org.apache.tika:tika-core:2.0.0,org.apache.tika:tika-parsers:2.0.0   --driver-memory 2G   --executor-memory 3G   --executor-cores 2   --conf "spark.yarn.maxAppAttempts=1"   --conf "spark.hadoop.fs.s3.canned.acl=BucketOwnerFullControl"   --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"   --conf "spark.dynamicAllocation.minExecutors=1"   --conf "spark.dynamicAllocation.maxExecutors=5"   --conf "spark.dynamicAllocation.initialExecutors=1"   --conf "spark.executor.memoryOverhead=1G"


//spark-shell --master yarn   --name spark-job-shell --packages org.apache.hudi:hudi-spark3.1-bundle_2.12:0.11.1,com.amazonaws:aws-java-sdk-glue:1.11.267,org.apache.spark:spark-avro_2.12:3.1.0,org.apache.avro:avro:1.8.2    --jars s3://pay2-raas-stg/applications/daas-recon-jobs/daas-recon-jobs-assembly-0.24.0.jar,/usr/lib/hadoop-hdfs/lib/jsch-0.1.54.jar    --driver-memory 5G   --executor-memory 6G   --executor-cores 4  --conf spark.yarn.maxAppAttempts=1     --conf spark.hadoop.fs.s3.canned.acl=BucketOwnerFullControl   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer     --conf spark.dynamicAllocation.minExecutors=1   --conf spark.dynamicAllocation.maxExecutors=10     --conf spark.dynamicAllocation.initialExecutors=1   --conf spark.executor.memoryOverhead=2G --conf spark.driver.maxResultSize=0   --conf spark.sql.autoBroadcastJoinThreshold=-1

object SimpleSparkApp {
  def main(args: Array[String]): Unit = {
//    Create a spark session

    val spark = SparkSession
      .builder()
      .appName("SimpleSparkApp")
      .master("local[*]")
      .getOrCreate()

    // Read a csv file
    val inputCSVFile = "data/example1.csv";
    val df: DataFrame = spark.read
      .option("header", "true")
      .csv(inputCSVFile)

    val resultDF: DataFrame = df.withColumn("new_column", lit("Hello World"))

    // Show the result
    resultDF.show()

    // Write the result
    val outputCSVFileName = "data/example1output"
    resultDF.write
      .mode(saveMode = "overwrite")
      .option("header", "true")
      .csv(outputCSVFileName)

    // Stop the Spark session
    spark.stop()
  }

}
