package day2

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Create_RDD_Memory {
  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务逻辑
    //1.创建RDD 并行RDD(业务列表,调用CPU核数分片处理)
    val rdd1 = sc.parallelize(List(1,2,3,4,5), 2).map(num => num * 2)
    //计算逻辑
    rdd1.collect().foreach(println)

    //关闭链接
    sc.stop()
  }
}