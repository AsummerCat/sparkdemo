package day5

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
 * 使用sparkSql 基础语法
 *
 * DataFrame
 * DataSet
 */
object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //1.读取资源

    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()

    //3.创建sql查询操作
    val df = sqlc.read.json("SparkWordCount\\src\\data\\user.json")
    //或者
//    val frame: DataFrame = sqlc.sql("select * from user")
    val list = immutable.List(List(df))

    //3.查询语句
    sqlc.sql("select * from user")
    list.foreach(print)
  }
}
