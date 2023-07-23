package com.mashibing.flinkjava.code.chapter6.serializer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
测试自定义注册Kryo的序列化器
 */
public class SerializerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //注册自定义的序列化器
        env.getConfig().registerTypeWithKryoSerializer(Student.class,StudentSerializer.class);
        DataStreamSource<String> source = env.socketTextStream("node05", 9999);
        SingleOutputStreamOperator<Student> operator = source.flatMap(new FlatMapFunction<String, Student>() {
            @Override
            public void flatMap(String line, Collector<Student> collector) throws Exception {
                String[] words = line.split(",");
                collector.collect(new Student(Integer.valueOf(words[0]), words[1], Integer.valueOf(words[2])));
            }
        });
        operator.print();
        env.execute();
    }
}
