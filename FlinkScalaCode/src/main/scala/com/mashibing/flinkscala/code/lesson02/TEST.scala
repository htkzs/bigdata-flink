package com.mashibing.flinkscala.code.lesson02
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment, TableResult}

import java.util.concurrent.TimeUnit

//连接mysql 有个JVM 报错，看这个 https://blog.csdn.net/qq_41078608/article/details/124625998
//需要修改 路径 L:\Program Files\Java\jdk-11.0.16\conf\security 中的 java.security 配置文件

//https://www.mashibing.com/question/detail/30478
object TEST {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      //.inBatchMode()
      .build()
    val tblEnv: TableEnvironment = TableEnvironment.create(settings)

    tblEnv.executeSql(
      """
        |CREATE TABLE myperson1 (
        |  id INT,
        |  name STRING,
        |  age INT
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://192.168.179.5:3306/mydb?useSSL=false',
        |   'username' = 'root',
        |   'password' = '123456',
        |   'table-name' = 'person'
        |);
        |""".stripMargin)

//    val table: Table = tblEnv.sqlQuery("select * from myperson1")
//    table.execute().print()

    val result: TableResult = tblEnv.executeSql("SELECT count(id) from myperson1")
    result.print()

//    tblEnv.executeSql(
//      """
//        |CREATE TABLE myperson2 (
//        |  id INT,
//        |  name STRING,
//        |  age INT,
//        |  PRIMARY KEY (id) NOT ENFORCED
//        |) WITH (
//        |   'connector' = 'jdbc',
//        |   'url' = 'jdbc:mysql://192.168.179.5:3306/test_ch?useSSL=false',
//        |   'username' = 'root',
//        |   'password' = '123456',
//        |   'table-name' = 'person2'
//        |);
//        |""".stripMargin)



//    val result: TableResult = tblEnv.executeSql(
//      """
//        | INSERT INTO myperson2 (id,name,age)
//        | SELECT id,name,age FROM myperson1
//        |""".stripMargin)

//    result.print()

  }
}
