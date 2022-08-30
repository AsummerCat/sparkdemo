package day6

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * SparkSql 数据的加载和保存
 */
object SparkLoadFile {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()

    //直接加载数据 并且转换为DataFrame
    sqlc.sql("select * from json.`/opt/module/data/user.json`").show

    //加载数据
    val frame = sqlc.read.json("xxx/1.json")
    //保存数据  ->保存为 parquet 格式
    frame.write.mode("append").save("/opt/module/data/output")


    //读取mysql数据
    //方式 1：通用的 load 方法读取
    sqlc.read.format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .load().show

    //方式 2:通用的 load 方法读取 参数另一种形式
    sqlc.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123", "dbtable" -> "user", "driver" -> "com.mysql.jdbc.Driver")).load().show

    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    val df: DataFrame = sqlc.read.jdbc("jdbc:mysql://linux1:3306/spark-sql", "user", props)
    df.show

    //释放资源
    sqlc.stop()


  }
}
