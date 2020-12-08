package com.demo.transformation;

import com.demo.pojo.Sensor;
import com.demo.wc.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RollingAggregation {

    public static void main(String[] args) {
            //创建执行流处理环境
            StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
            //从文件中读取数据
            String inputPath = "C:\\Users\\Administrator\\Desktop\\文档\\青州项目\\opc服务采集数据\\U_MAK01_DC_DQYSL.txt";
            DataStream<String> inputDataStream = executionEnvironment.readTextFile(inputPath);
            //转换成sensor类型
            DataStream<Sensor> dataStream = inputDataStream.map(new MapFunction<String, Sensor>() {
                public Sensor map(String s) throws Exception {
                    return new Sensor("sesnor" +s,s);
                }
            });

        }

        //自定义类实现FlatMapper结构
        public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
            public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                String word = s;
                collector.collect(new Tuple2<String,Integer>(word,1));

            }
        }
    }



