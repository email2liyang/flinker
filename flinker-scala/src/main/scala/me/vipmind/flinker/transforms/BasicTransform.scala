package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.scala._

object BasicTransform {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75))
      .map(s => Student(s.name.toLowerCase, s.year, s.score))
      .filter(s => s.score > 70)
      .flatMap(s => List(s, s))
      .print()

    env.execute("basic stream")
  }
}
