package day5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 使用sparkSql 基础语法
 *
 * DataFrame
 * DataSet
 */
object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //3.创建sql查询操作
    val df = sqlc.read.json("SparkWordCount\\src\\data\\user.json")
    //或者
    //    val frame: DataFrame = sqlc.sql("select * from user")
    df.show()

  }
}
