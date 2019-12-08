package me.vipmind.flinker.functions

import me.vipmind.flinker.transforms.Student
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object ReduceFn {

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
      .reduce((s1, s2) => {
        if (s1.score > s2.score) {
          s2
        } else {
          s1
        }
      })

    stream.print()
    env.execute()
  }
}
