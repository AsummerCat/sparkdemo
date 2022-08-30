package day5

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

/**
 * sprakSql自定义函数扩展  UDAF强类型 按照属性来操作
 */
object SparkSqlCustomFunUDAF1 {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.导入隐式转换包 后面很多操作都需要它

    //4.获取数据源
    val df = sqlc.read.json("SparkWordCount\\src\\data\\user.json")

    //5.创建临时表
    df.createOrReplaceTempView("user")

    //6.创建udf自定义函数 并注册
    //在 spark 中注册聚合函数 求平均
    sqlc.udf.register("avgAge", functions.udaf(new MyAveragUDAF()))

    //7.编写查询语句并且加入自定义函数
    sqlc.sql("Select avgAge(age) from user").show()



    //关闭连接
    sqlc.close();
  }


  /*
   * 定义类继承 org.apache.spark.sql.expressions.Aggregator，并重写其中方法
   * IN: 输入数据类型
   * BUF: 缓冲区 中间处理的数据类型
   * OUT: 输出的数据类型
   */
  case class Buff(var total: Long, var count: Long)

  class MyAveragUDAF extends Aggregator[Long, Buff, Long] {
    //初始值 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    //根据输入的数据来更新缓冲区的额数据
    override def reduce(b: Buff, a: Long): Buff = {
      b.total = b.total + a
      b.count = b.count + 1
      b
    }

    //合并缓冲区
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.count = b1.count + b2.count
      b1.total = b1.total + b2.total
      b1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count

    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }


}
