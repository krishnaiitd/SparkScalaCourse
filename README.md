# How to run the program on local machine

/Users/krishna/Documents/ApacheSparkWithScala/spark-3.3.2-bin-hadoop3/bin/spark-submit --class com.sundogsoftware.spark.HelloWorld  out/artifacts/SparkCourse/SparkCourse.jar



# to start hadoop locally 

hadoop namenode -format


cd /Users/krishna/Documents/ApacheSparkWithScala/spark-3.3.2-bin-hadoop3


start-all.sh



# Check the existing files on hdfs

hdfs dfs -ls

# Create directory first 


hadoop fs -mkdir -p /user/krishna/data

# copy from your local machine to hdfs

hadoop fs -put data/1800.csv hdfs://0.0.0.0:9000/user/krishna/data/

# Check the exist data:

➜  SparkScalaCourse git:(main) ✗ hdfs dfs -ls /user/krishna/
2023-03-03 21:12:36,071 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 1 items
drwxr-xr-x   - krishna supergroup          0 2023-03-03 20:52 /user/krishna/data






