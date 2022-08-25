package day3

import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD 设置检查点
 */
object Spark03_RDD_checkPoint {
  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //1.设置检查点保存路径
    sc.setCheckpointDir("cp")

    //执行业务逻辑
    //2.创建RDD 并行RDD(业务列表,调用CPU核数分片处理)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5), 2)

    //3.设置检查点
    //需要落盘,需要指定检查点保存路径
    //cache()在job执行完毕后会自动删除,而 检查点在job执行完毕后还存在
    rdd1.cache();
    rdd1.checkpoint();


    //计算逻辑1
    val value = rdd1.map(num=>{
      println("xxxxxxxxx")
      num*2
    })
    value.collect().foreach(print)
    println("")
    println("****************************")

    //计算逻辑2
    val value1 = rdd1.map(num=>{
      println("xxxxxxxxx")
      num*3
    })
    value1.collect().foreach(print)

    //关闭链接
    sc.stop()
  }
}