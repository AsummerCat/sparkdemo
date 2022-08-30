package day8

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * SparkStreaming 保留采集周期历史数据 进行统计
 */
object SparkStreamingState {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3.设置检查点
    ssc.checkpoint("/historyCache")

    //3.获取采集周期数据
    //某些场合下,需要历史采集周期数据,进行数据汇总 需要设置检查点来保留历史数据
    val lineStreams = ssc.socketTextStream("127.0.0.1", 9999)

    val value = lineStreams.map((_, 1))
    value.print()

    //updateStateByKey: 根据key对数据的状态进行更新
    //传递的参数中含有两个值
    //第一个值表示相同的key的value数据
    //第二个值表示缓存区相同key的value数据
    value.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()

  }
}
