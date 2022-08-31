package day9

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * spark 查看执行计划
 *
 */
object SparkExplain {
  def main(args: Array[String]): Unit = {
    //1.读取配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //2.创建SQLcontext对象
    val sqlc = SparkSession.builder().config(sparkConf).getOrCreate()
    //3.导入隐式转换包 后面很多操作都需要它

    //4.获取数据源
    val df = sqlc.read.json("SparkWordCount\\src\\data\\user.json")

    //5. 显示表格
    df.show()

    //6.使用 DataFrame =>Sql
    //创建一个临时视图
    df.createOrReplaceTempView("user")
    //编写sql语句
    sqlc.sql("select name,age from user").explain()

    println("=====================================explain()-只展示物理执行计划============================================")
    sqlc.sql("select name,age from user").explain()

    println("===============================explain(mode = \"simple\")-只展示物理执行计划=================================")
    sqlc.sql("select name,age from user").explain(mode = "simple")

    println("============================explain(mode = \"extended\")-展示逻辑和物理执行计划==============================")
    sqlc.sql("select name,age from user").explain(mode = "extended")

    println("============================explain(mode = \"codegen\")-展示可执行java代码===================================")
    sqlc.sql("select name,age from user").explain(mode = "codegen")

    println("============================explain(mode = \"formatted\")-展示格式化的物理执行计划=============================")
    sqlc.sql("select name,age from user").explain(mode = "formatted")

    //最后关闭连接
    sqlc.close();
  }


  case class User(age: Int, name: String, address: Int)
}
