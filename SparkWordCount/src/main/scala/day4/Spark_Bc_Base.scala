package day4

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量的使用(只读变量) 基础 sc.broadcast(list)
 */
object Spark_Bc_Base {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务逻辑
    //1.创建RDD

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))

    //2.新增广播变量
    val list = List(2, 4)
    val bc = sc.broadcast(list)

    rdd.map(
      num => {
        // 获取广播变量的内容
        //3.判断rdd中的值 如果为2,4则*10倍
        if (bc.value.contains(num)) {
          num * 10
        } else {
          num
        }
      }).collect().foreach(println)

  }

}
