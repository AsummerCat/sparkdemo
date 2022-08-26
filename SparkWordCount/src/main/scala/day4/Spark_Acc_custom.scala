package day4

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 自定义累加器的使用 AccumulatorV2
 */
object Spark_Acc_custom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //执行业务逻辑
    //1.创建RDD
    val rdd = sc.makeRDD(List("hello", "world", "test"))

    //2.创建自定义累加器对象
    val acc = new Spark_CustomACC()
    //3.注册并且构建累加器
    sc register(acc, "custom")

    //4.处理业务逻辑
    rdd.foreach(
      num => {
        // 使用累加器
        acc.add(num)

      })
    // 获取累加器的值
    println("sum = " + acc.value)

  }


  // 自定义累加器
  // 1. 继承 AccumulatorV2，并设定泛型
  // 2. 重写累加器的抽象方法
  class Spark_CustomACC extends AccumulatorV2[String, mutable.Map[String, Long]] {
    var map: mutable.Map[String, Long] = mutable.Map()

    // 累加器是否为初始状态
    override def isZero: Boolean = {
      map.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new Spark_CustomACC
    }

    // 重置累加器
    override def reset(): Unit = {
      map.clear()
    }

    // 向累加器中增加数据 (In)
    override def add(word: String): Unit = {
      // 查询 map 中是否存在相同的单词
      // 如果有相同的单词，那么单词的数量加 1
      // 如果没有相同的单词，那么在 map 中增加这个单词
      map(word) = map.getOrElse(word, 0L) + 1L
    }

    // 合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]):
    Unit = {
      val map1 = map
      val map2 = other.value
      // 两个 Map 的合并
      map = map1.foldLeft(map2)(
        (innerMap, kv) => {
          innerMap(kv._1) = innerMap.getOrElse(kv._1, 0L) + kv._2
          innerMap
        }
      )
    }

    // 返回累加器的结果 （Out）
    override def value: mutable.Map[String, Long] = map
  }

}

