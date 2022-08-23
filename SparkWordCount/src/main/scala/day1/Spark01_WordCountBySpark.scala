package day1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * 基础使用求和 使用RDD语法
 */
/**
 * 使用spark的功能实现
 */
object Spark01_WordCountBySpark {

  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务逻辑

    //1.读取文件,获取一行一行的数据
    val lines: RDD[String] = sc.textFile("SparkWordCount\\src\\data\\1.txt")

    //2.将一行大数据进行拆分,形成一个个的单词 按照空格切割
    val word: RDD[String] = lines.flatMap(_.split(" "));

    val wordToOne = word.map {
      word => (word, 1)
    }

    //Spark提供的功能,相同的Key的数据,可以对value进行聚合
    val wordGroup1: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)


    //5. 将结果采集打印
    val array: Array[(String, Int)] = wordGroup1.collect()
    array.foreach(println)

    //关闭链接
    sc.stop()
  }
}
