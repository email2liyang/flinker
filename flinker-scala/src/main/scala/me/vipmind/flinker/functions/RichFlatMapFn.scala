package me.vipmind.flinker.functions

import me.vipmind.flinker.transforms.Student
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFlatMapFn {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75))
      .flatMap(new StudentFlatMapFn)

    stream.print()
    env.execute()
  }
}

class StudentFlatMapFn extends RichFlatMapFunction[Student, (Int, Student)] {

  @transient
  var subTaskIndex = 0

  override def open(config: Configuration): Unit = subTaskIndex = getRuntimeContext.getIndexOfThisSubtask

  override def close(): Unit = super.close()

  override def flatMap(in: Student,
                       collector: Collector[(Int, Student)]): Unit = {
    for (idx <- 0 to subTaskIndex) {
      collector.collect(idx, in)
    }
  }
}
