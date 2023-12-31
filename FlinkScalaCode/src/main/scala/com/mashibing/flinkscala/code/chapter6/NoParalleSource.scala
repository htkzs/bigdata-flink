package com.mashibing.flinkscala.code.chapter6

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Random

/**
 * Flink读取自定义Source，并行度为1
 */
class MyDefinedNoParalleSource extends SourceFunction[StationLog]{
  var flag = true

  /**
   * 主要方法:启动一个Source，大部分情况下都需要在run方法中实现一个循环产生数据
   * 这里计划每次产生10条基站数据
   */
  override def run(ctx: SourceFunction.SourceContext[StationLog]): Unit = {
    val random = new Random()
    val callTypes = Array[String]("fail", "success", "busy", "barring")

    while (flag) {
      val sid = "sid_" + random.nextInt(10)
      val callOut = "1811234" + (random.nextInt(9000) + 1000)
      val callIn = "1915678" + (random.nextInt(9000) + 1000)
      val callType = callTypes(random.nextInt(4))
      val callTime = System.currentTimeMillis()
      val durations = random.nextInt(50).toLong
      ctx.collect(StationLog(sid, callOut, callIn, callType, callTime, durations))

      Thread.sleep(1000) //每条数据暂停1s
    }
  }

  //当取消对应的Flink任务时被调用
  override def cancel(): Unit = {
    flag = false
  }
}

object NoParalleSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val ds: DataStream[StationLog] = env.addSource(new MyDefinedNoParalleSource)
    ds.print()
    env.execute()

  }
}
