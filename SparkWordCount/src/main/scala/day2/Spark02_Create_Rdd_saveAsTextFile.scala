package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * RDD读取文件并保存分区文件
 * saveAsTextFile
 */
object Spark02_Create_Rdd_saveAsTextFile {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    //读取文件 并设置2个分区
    val rdd: RDD[(String, String)] = sparkContext.wholeTextFiles("SparkWordCount\\src\\data\\1.txt",2)
//    rdd.collect().foreach(println)
    rdd.saveAsTextFile("SparkWordCount\\src\\data\\output")
    sparkContext.stop()
  }


}
