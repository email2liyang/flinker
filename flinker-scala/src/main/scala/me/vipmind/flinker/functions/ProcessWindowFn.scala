package me.vipmind.flinker.functions

import me.vipmind.flinker.transforms.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowFn {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Student] {
        override def getCurrentWatermark: Watermark = new Watermark(System.currentTimeMillis())

        override def extractTimestamp(t: Student, l: Long): Long = System.currentTimeMillis()
      })
      .keyBy(_.year)
      .timeWindow(Time.seconds(1))
      .process(new MinMaxScoreOfYearFunction())

    stream.print()
    env.execute()
  }
}

case class MinMaxScoreOfYear(year: Int, min: Int, max: Int, endTime: Long)

class MinMaxScoreOfYearFunction extends ProcessWindowFunction[Student, MinMaxScoreOfYear, Int, TimeWindow] {

  override def process(key: Int, context: Context,
                       elements: Iterable[Student],
                       out: Collector[MinMaxScoreOfYear]): Unit = {
    val scores = elements.map(_.score)
    val windowEnd = context.window.getEnd
    out.collect(MinMaxScoreOfYear(key, scores.min, scores.max, windowEnd))
  }
}
