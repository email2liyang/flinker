package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.scala._
object KeyedStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(String, List[String])] = env.fromElements(
      ("cn", List("Beijing")),
      ("au", List("Sydney")),
      ("cn", List("ShangHai")),
      ("au", List("Perth")),
      ("cn", List("Tianjin")))
    val resultStream = inputStream.keyBy(0).reduce((x, y) => (x._1, x._2 ::: y._2))
    resultStream.print

    env.execute("keyed stream")
  }
}
