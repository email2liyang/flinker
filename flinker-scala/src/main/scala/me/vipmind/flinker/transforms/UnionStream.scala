package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.scala._

object UnionStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val year2Stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Kelly", 2, 80),
      Student("Selina", 2, 75))
    val year3Stream = env.fromElements(
      Student("Emily", 3, 60),
      Student("Mia", 3, 70))

    val allStudentStream = year2Stream.union(year3Stream)
      .map(s => Student(s.name.toLowerCase, s.year, s.score))


    allStudentStream.print()

    env.execute("union stream")
  }
}
