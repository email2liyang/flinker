package me.vipmind.flinker.transforms

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object PartitionedStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75)
    )
      .partitionCustom(new Partitioner[Int] {
        val r = scala.util.Random

        override def partition(key: Int, numPartitions: Int): Int = {
          if (key == 2) {
            0
          } else {
            r.nextInt(numPartitions)
          }
        }
      }, "year")


    stream.print()

    env.execute()
  }
}
