package day2

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Create_RDD_Memory {
  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务逻辑

    val rdd1 = sc.parallelize(List(1, 2, 3, 4), 2)

    rdd1.collect().foreach(println)

    //关闭链接
    sc.stop()
  }
}