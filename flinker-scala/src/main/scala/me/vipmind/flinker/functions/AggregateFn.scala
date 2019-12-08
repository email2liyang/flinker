package me.vipmind.flinker.functions

import me.vipmind.flinker.transforms.Student
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object AggregateFn {

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
      .aggregate(new AvgScoreFunction())

    stream.print()
    env.execute()
  }
}

class AvgScoreFunction extends AggregateFunction[Student, (Int, Int, Int), (Int, Double)] {

  override def createAccumulator(): (Int, Int, Int) = (0, 0, 0) //(year,studentCount, totalScore)

  override def add(in: Student, acc: (Int, Int, Int)): (Int, Int, Int) = (in.year, acc._2 + 1, acc._3 + in.score)

  override def getResult(acc: (Int, Int, Int)): (Int, Double) = (acc._1, acc._3 / acc._2)

  override def merge(acc1: (Int, Int, Int), acc2: (Int, Int, Int)): (Int, Int, Int) = (acc1._1, acc1._2 + acc2._2, acc1._3 +
                                                                                                                   acc2._3)
}
