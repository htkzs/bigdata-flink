package com.mashibing.flinkjava.code.transformations;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/*
 * @Author GhostGalaxy
 * @Description //map算子测试
 * @Date 16:18:56 2023/7/23
 * @Param
 * @return
 **/
public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = environment.fromCollection(Arrays.asList(1, 2, 3, 4));
        SingleOutputStreamOperator<Integer> streamOperator = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer num) throws Exception {
                return num + 1;
            }
        });
        streamOperator.print();
        environment.execute();
    }
    public void map(){
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = environment.fromCollection(Arrays.asList(1, 2, 3, 4));
        SingleOutputStreamOperator<Integer> streamOperator = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer num) throws Exception {
                return num+1;
            }
        }, new TypeInformation<Integer>() {
            @Override
            public boolean isBasicType() {
                return false;
            }

            @Override
            public boolean isTupleType() {
                return false;
            }

            @Override
            public int getArity() {
                return 0;
            }

            @Override
            public int getTotalFields() {
                return 0;
            }

            @Override
            public Class<Integer> getTypeClass() {
                return null;
            }

            @Override
            public boolean isKeyType() {
                return false;
            }

            @Override
            public TypeSerializer<Integer> createSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public String toString() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public boolean canEqual(Object o) {
                return false;
            }
        });
    }
}
