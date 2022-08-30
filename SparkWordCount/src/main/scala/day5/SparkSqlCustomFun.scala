package day5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * sprakSql自定义函数扩展
 */
object SparkSqlCustomFun {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.导入隐式转换包 后面很多操作都需要它
    import sqlc.implicits._

    //4.获取数据源
    val df = sqlc.read.json("SparkWordCount\\src\\data\\user.json")

    //5.创建临时表
    df.createOrReplaceTempView("user")

    //6.创建udf自定义函数 并注册
    sqlc.udf.register("addName", (x: String) => "Name:" + x)

    //7.编写查询语句并且加入自定义函数
    sqlc.sql("Select addName(name) as name,age from user").show()


    //关闭连接
    sqlc.close();
  }
}
