package me.vipmind.flinker.transforms

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      Student("Amy", 2, 100),
      Student("Emily", 3, 60),
      Student("Kelly", 2, 80),
      Student("Mia", 3, 70),
      Student("Selina", 2, 75))
      .process(new StudentYearProcessor())

    val year2StudentStream = stream.getSideOutput(new OutputTag[String]("year2 students"))
      .map(_.toLowerCase)
    val year3StudentStream = stream.getSideOutput(new OutputTag[String]("year3 students"))
      .map(_.toUpperCase)

    year2StudentStream.print()
    year3StudentStream.print()

    env.execute()
  }

}

class StudentYearProcessor extends ProcessFunction[Student, Student] {

  lazy val year2StudentOutput: OutputTag[String] = new OutputTag[String]("year2 students")
  lazy val year3StudentOutput: OutputTag[String] = new OutputTag[String]("year3 students")

  override def processElement(student: Student,
                              context: ProcessFunction[Student, Student]#Context,
                              collector: Collector[Student]): Unit = {
    if (student.year == 2) {
      context.output(year2StudentOutput, student.name)
    }
    if (student.year == 3) {
      context.output(year3StudentOutput, student.name)
    }
    collector.collect(student)
  }
}
