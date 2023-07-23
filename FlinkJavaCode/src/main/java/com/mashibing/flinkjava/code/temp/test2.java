package com.mashibing.flinkjava.code.temp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String  createSQL = "CREATE TABLE KafkaTable (" +
                "  `name` STRING," +
                "  `ts` BIGINT," +
                "  `ts_1` AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "  WATERMARK FOR ts_1 AS ts_1 - INTERVAL '1' SECOND " +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'testtopic'," +
                "  'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")";
        //创建表
        tableEnv.executeSql(createSQL);

        tableEnv.executeSql("desc KafkaTable").print();

        String executeSql = "SELECT" +
                "  name," +
                "  TUMBLE_START(ts_1, INTERVAL '5' SECOND) AS wStart," +
                "  COUNT(1) " +
                " FROM KafkaTable  " +
                "  GROUP BY " +
                "  TUMBLE(ts_1, INTERVAL '5' SECOND)," +
                "  name";

        Table table = tableEnv.sqlQuery(executeSql);
        tableEnv.toChangelogStream(table).print("xx");

//        String executeSql = "SELECT window_start, window_end, COUNT(name)" +
//                "  FROM TABLE(" +
//                "    HOP(TABLE KafkaTable, DESCRIPTOR(ts_1), INTERVAL '5' SECOND, INTERVAL '10' SECOND))" +
//                "  GROUP BY window_start, window_end";
//        TableResult tableResult = tableEnv.executeSql(executeSql);
//        tableResult.print();
//        tableEnv.executeSql("select * from KafkaTable").print();


        env.execute();
    }
}
