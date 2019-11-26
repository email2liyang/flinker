package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.scala._

object ConnectStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val studentStream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Selina", 2, 75))
    val teacherStream = env.fromElements(
      Teacher("Sally", 2, "Chinese"),
      Teacher("Jade", 2, "English")
    )

    val connectedStream: ConnectedStreams[Student, Teacher] = studentStream.connect(teacherStream)
    connectedStream.map(
      s => Student(s.name.toLowerCase(), s.year, s.score),
      t => Teacher(t.name.toLowerCase, t.year, t.course)
    )

    env.execute("connected stream")
  }
}
