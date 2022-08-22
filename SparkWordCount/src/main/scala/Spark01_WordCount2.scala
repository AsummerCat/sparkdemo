import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount2 {

  def main(args: Array[String]): Unit = {

    //创建scala和Spark框架的链接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    //执行业务逻辑

    //1.读取文件,获取一行一行的数据
    val lines: RDD[String] = sc.textFile("SparkWordCount\\src\\data\\1.txt")

    //2.将一行大数据进行拆分,形成一个个的单词 按照空格切割
    val word: RDD[String] = lines.flatMap(_.split(" "));

    val wordToOne = word.map {
      word => (word, 1)
    }


    //3.将数据根据单词进行分组,便于统计
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)


    //4.对分组后的数据进行转换 获取 (name,num)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => {
          (t1._1, t1._2+ t2._2)
        })
      }
    }

    //5. 将结果采集打印
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //关闭链接
    sc.stop()
  }
}
