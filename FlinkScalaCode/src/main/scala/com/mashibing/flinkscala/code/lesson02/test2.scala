package com.mashibing.flinkscala.code.lesson02

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util

object test2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Long] = env.generateSequence(1, 20).setParallelism(4)
    stream.keyBy()
    val list = new util.ArrayList[String]()
    val xx = new util.LinkedList[Int]()
  }
}
