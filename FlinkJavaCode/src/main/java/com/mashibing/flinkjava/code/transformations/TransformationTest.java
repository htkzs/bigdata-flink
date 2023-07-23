package com.mashibing.flinkjava.code.transformations;

import com.mashibing.flinkjava.code.chapter6.StationLog;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/*
 * @Author GhostGalaxy
 * @Description //fink的Transformation各种算子测试 包含flatmap
 * @Date 16:46:42 2023/7/23
 * @Param
 * @return
 **/
public class TransformationTest {
    private static final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    /*
     * @Author GhostGalaxy
     * @Description //测试flatMap 扁平化数据处理
     * @Date 16:53:31 2023/7/23
     * @Param []
     * @return void
     **/
    public static void flatMapTest() throws Exception {
        DataStreamSource<String> datasource = environment.fromCollection(Arrays.asList("1,2", "3,4", "5,6", "7,8"));
        SingleOutputStreamOperator<Integer> streamOperator = datasource.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String str, Collector<Integer> collector) throws Exception {
                String[] strings = str.split(",");
                for (String s : strings) {
                    collector.collect(Integer.valueOf(s));
                }
            }
        });
        streamOperator.print();
        environment.execute();
    }
    /*
     * @Author GhostGalaxy
     * @Description //Filter算子测试
     * @Date 16:54:31 2023/7/23
     * @Param []
     * @return void
     **/
    public static void FilterTest() throws Exception {
        DataStreamSource<Integer> source = environment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        SingleOutputStreamOperator<Integer> filter = source.filter(new FilterFunction<Integer>() {
            //返回true类型的数据将被保存下来
            @Override
            public boolean filter(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        filter.print();
        environment.execute();
    }
    /*
     * @Author GhostGalaxy
     * @Description //keyBy算子测试
     * @Date 16:58:50 2023/7/23
     * @Param []
     * @return void
     **/
    public static void keyByTest() throws Exception {
        DataStreamSource<Tuple2<String, Integer>> streamSource = environment.fromCollection(Arrays.asList(Tuple2.of("a", 1), Tuple2.of("b", 1),
                Tuple2.of("c", 1), Tuple2.of("a", 2),
                Tuple2.of("b", 2), Tuple2.of("c", 2)));
        //根据key进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        //根据分组进行求和
        keyedStream.sum(1).print();
        environment.execute();
    }
    /*
     * @Author GhostGalaxy
     * @Description //聚合Aggregations算子 sum min max minBy maxBy
     * @Date 17:26:08 2023/7/23
     * @Param
     * @return
     **/
    public static void AggregationsTest(){
        DataStreamSource<StationLog> streamSource = environment.fromCollection(Arrays.asList(new StationLog("callID1", "17371167564", "18391585902", "fail", System.currentTimeMillis(), 30L),
                new StationLog("callID1", "15098765674", "18391585902", "success", System.currentTimeMillis(), 50L),
                new StationLog("callID1", "17876578901", "17778945673", "busy", System.currentTimeMillis(), 80L),
                new StationLog("callID2", "18791585902", "18391585902", "barring", System.currentTimeMillis(), 150L),
                new StationLog("callID3", "18395476031", "13991742501", "success", System.currentTimeMillis(), 70L),
                new StationLog("callID2", "17789045671", "16578930451", "success", System.currentTimeMillis(), 300L)
        ));
        //首先根据sid进行分组，再根据duration进行聚合
        KeyedStream<StationLog, String> keyedStream = streamSource.keyBy(new KeySelector<StationLog, String>() {
            @Override
            public String getKey(StationLog stationLog) throws Exception {
                return stationLog.sid;
            }
        });
        keyedStream.sum("duration").print();
        keyedStream.max("duration").print();
        keyedStream.min("duration").print();
        keyedStream.maxBy("duration").print();
        keyedStream.minBy("duration").print();
    }
    /*
     * @Author GhostGalaxy
     * @Description //测试reduce算子 reduce不许基于keyBy操作
     * @Date 17:49:51 2023/7/23
     * @Param []
     * @return void
     **/
    public static void reduceTest() throws Exception {
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = environment.fromCollection(Arrays.asList(Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("b", 1), Tuple2.of("c", 1), Tuple2.of("b", 3)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = dataStreamSource.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });
        reduce.print();
        environment.execute();
    }
    /*
     * @Author GhostGalaxy
     * @Description //流合并处理算子 union和connect union适合两个或者多个同数据类型的流数据进行合并为一个流数据  connect只能将两个数据类型不同的流合并成数据类型一致的流
     * @Date 18:16:40 2023/7/23
     * @Param [args]
     * @return void
     **/

    public static void UnionTest() throws Exception {
        DataStreamSource<String> st1 = environment.fromCollection(Arrays.asList("zookeeper", "kafka", "hadoop", "hbase", "spark", "flink"));
        DataStreamSource<String> st2 = environment.fromCollection(Arrays.asList("spring", "redis", "rabbit", "rocket", "springboot", "jvm"));
        DataStream<String> dataStream = st1.union(st2);
        dataStream.print();
        environment.execute();
    }

    public static void ConnectTest() throws Exception {
        DataStreamSource<Tuple2<String, String>> source1 = environment.fromCollection(Arrays.asList(Tuple2.of("stuId", "100515"),
                Tuple2.of("stuName", "zhangsan"), Tuple2.of("stuAge", "23"), Tuple2.of("stuSex", "man")));
        DataStreamSource<String> source2 = environment.fromCollection(Arrays.asList("hello world", "hello hadoop", "hello hbase"));
        ConnectedStreams<Tuple2<String, String>, String> connectedStreams = source1.connect(source2);
        /*
         * @Author GhostGalaxy
         * @Description //TODO
         * @Date 18:39:34 2023/7/23
         * @Param [] Tuple2<String, String> 第一个数据流的类型  String第二个数据流的类型 Object合并后将要返回的流的数据类型
         * @return void
         **/
        SingleOutputStreamOperator<Object> streamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, String>, String, Object>() {
            //用于处理第一个流
            @Override
            public Object map1(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2.f0 + "-" + stringStringTuple2.f1;
            }

            //用于处理第二个流
            @Override
            public Object map2(String s) throws Exception {
                return s + "*";
            }
        });
        streamOperator.print();
        environment.execute();
    }
    /*
     * @Author GhostGalaxy
     * @Description //iterate 迭代算子
     * @Date 19:17:18 2023/7/23
     * @Param [args]
     * @return void
     **/

    public static void iterateTest() throws Exception {
        //从socket中获取流数据
        DataStreamSource<String> source = environment.socketTextStream("node05", 9999);
        SingleOutputStreamOperator<Integer> st1 = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.valueOf(s);
            }
        });
        IterativeStream<Integer> iterate = st1.iterate();
        //定义迭代逻辑
        SingleOutputStreamOperator<Integer> st2 = iterate.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer num) throws Exception {
                return num - 1;
            }
        });
        //定义迭代结束的条件
        SingleOutputStreamOperator<Integer> filter = st2.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer num) throws Exception {
                return num > 0;
            }
        });
        //对迭代流应用条件
        iterate.closeWith(filter);
        filter.print();
        environment.execute();
    }
    public static void main(String[] args) throws Exception {
        TransformationTest.flatMapTest();
        iterateTest();


    }
}
