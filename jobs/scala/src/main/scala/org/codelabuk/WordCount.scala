package org.codelabuk
import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Word Count")
      .master("spark://spark-master:7077").getOrCreate()

    val sc = spark.sparkContext
    val txtData = sc.parallelize(List("Hello Spark", "Hello Scala",  "Hello Airflow", "Hello Docker" ,"Hello CodeLabuk"))
    val counts = txtData.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_ + _)

    counts.collect().foreach(println)
    spark.stop()

  }
}
