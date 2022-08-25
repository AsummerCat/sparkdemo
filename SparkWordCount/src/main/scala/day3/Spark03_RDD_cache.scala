package day3

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD使用缓存
 */
object Spark03_RDD_cache {
  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务逻辑
    //1.创建RDD 并行RDD(业务列表,调用CPU核数分片处理)
    val rdd1 = sc.parallelize(List(1,2,3,4,5), 2)

    //2.加入缓存节点
    rdd1.cache();
    //或者使用
    rdd1.persist(StorageLevel.MEMORY_ONLY);
    /**
     * *.chae() 默认持久化操作,只能将数据保存到内存中
       如果想要保存磁盘文件或者其他介质中,可用
       *.persist(StorageLevel.MEMORY_ONLY);
        持久化操作必须在 collect()之后才会产生数据
     */


    //计算逻辑1
    val value = rdd1.map(_ * 2)
    value.collect().foreach(print)
    println("")
    println("****************************")

    //计算逻辑2
    val value1 = rdd1.map(_ * 3)
    value1.collect().foreach(print)
    //关闭链接
    sc.stop()
  }
}