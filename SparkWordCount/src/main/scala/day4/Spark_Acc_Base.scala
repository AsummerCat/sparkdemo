package day4

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 累加器的使用(可变变量) 基础  var sum = sc.longAccumulator("sum")
 */
object Spark_Acc_Base {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务逻辑
    //1.创建RDD

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 声明累加器 系统内置三中累加器 1.longAccumulator 2.doubleAccumulator   3.collectionAccumulator
    var sum = sc.longAccumulator("sum")
    //    var sum = sc.doubleAccumulator("sum")
    //    var sum = sc.collectionAccumulator("sum")

    //系统自带了double和collection的累加器
    rdd.foreach(
      num => {
        // 使用累加器
        sum.add(num)

      })
    // 获取累加器的值
    println("sum = " + sum.value)

  }

}
