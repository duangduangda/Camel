package org.dean.camel

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


/**
  * 单词统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.windowing.time.Time

    // 1. 指定执行环境设定
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 指定数据源地址，读取输入数据
    val text = environment.socketTextStream("localhost", 9999)
    val counts = text
      .flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
    counts.print()
    environment.execute()
  }
}
