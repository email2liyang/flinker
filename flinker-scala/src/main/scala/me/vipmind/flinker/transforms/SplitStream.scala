package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.scala._

object SplitStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val splitStream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75))
      //split is deprecated since flink 1.6 use side out put instead
      .split(s => s.year match {
        case 2 => Seq("y2")
        case 3 => Seq("y3")
      })

    val y2Stream = splitStream.select("y2")
      .map(s => Student(s.name.toLowerCase, s.year, s.score))
    val y3Stream = splitStream.select("y3")
      .map(s => Student(s.name.toUpperCase, s.year, s.score))

    y2Stream.print()
    y3Stream.print()

    env.execute("Split stream")
  }
}
