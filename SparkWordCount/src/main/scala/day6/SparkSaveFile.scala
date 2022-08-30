package day6

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.Properties

/**
 * SparkSql 数据的保存
 */
object SparkSaveFile {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.导入隐式转换包 后面很多操作都需要它
    import sqlc.implicits._

    val rdd: RDD[User] = sqlc.sparkContext.makeRDD(List(User("lisi", 20), User("zs", 30)))
    val ds: Dataset[User] = rdd.toDS

    //方式 1：通用的方式 format 指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux1:3306/spark-sql")
      .option("user", "root")
      .option("password", "123123")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    //方式 2：通过 jdbc 方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123123")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux1:3306/spark-sql",
      "user", props)
    //释放资源
    sqlc.stop()


  }

  case class User(var name: String, var age: Long)

}
