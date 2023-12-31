package com.mashibing.flinkjava.code.chapter6;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Flink 读取集合中数据得到 DataStream
 */
public class CollectionSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<StationLog> stationLogArrayList = new ArrayList<StationLog>();
        stationLogArrayList.add(new StationLog("001", "186", "187", "busy", 1000L, 0L));
        stationLogArrayList.add(new StationLog("002", "187", "186", "fail", 2000L, 0L));
        stationLogArrayList.add(new StationLog("003", "186", "188", "busy", 3000L, 0L));
        stationLogArrayList.add(new StationLog("004", "188", "186", "busy", 4000L, 0L));
        stationLogArrayList.add(new StationLog("005", "188", "187", "busy", 5000L, 0L));
        DataStreamSource<StationLog> dataStreamSource = env.fromCollection(stationLogArrayList);
        dataStreamSource.print();
        env.execute();
    }
    //可以不必先创建集合
    public void CollectionSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<StationLog> streamSource = env.fromCollection(Arrays.asList(new StationLog("001", "186", "187", "busy", 1000L, 0L),
                new StationLog("002", "187", "186", "fail", 2000L, 0L)));
        streamSource.print();
        env.execute();
    }
}
