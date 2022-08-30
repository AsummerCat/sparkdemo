package day8

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming 使用Window来创建时间窗口 进行多个采集周期数据 统一处理
 */
object SparkStreamingWindow {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    //3.获取采集周期数据
    //某些场合下,需要历史采集周期数据,进行数据汇总 需要设置检查点来保留历史数据
    val lineStreams = ssc.socketTextStream("127.0.0.1", 9999)

    val data = lineStreams.map((_, 1))

    //设置时间滑动窗口 3 秒一个批次，窗口 12 秒，滑步 6 秒。
    val wordCounts = data.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(6))

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
