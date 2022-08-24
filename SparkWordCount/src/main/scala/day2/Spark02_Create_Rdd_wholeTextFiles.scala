package day2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * RDD读取文件并获取文件路径
 * wholeTextFiles
 */
object Spark02_Create_Rdd_wholeTextFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    //读取文件并获取文件路径
    //读取的结果为元组,第一个元素表示文件的路径,第二个元素表示文件内容
    val rdd: RDD[(String, String)] = sparkContext.wholeTextFiles("SparkWordCount\\src\\data\\1.txt")
    rdd.collect().foreach(println)
    sparkContext.stop()
  }


}
