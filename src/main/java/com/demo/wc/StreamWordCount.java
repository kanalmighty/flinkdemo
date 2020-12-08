package com.demo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建执行流处理环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputPath = "C:\\Users\\Administrator\\Desktop\\文档\\青州项目\\opc服务采集数据\\U_MAK01_DC_DQYSL.txt";
        DataStream<String> inputDataStream = executionEnvironment.readTextFile(inputPath);
        //按照第一个位置分组
        DataStream<Tuple2<String,Integer>> sum = inputDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);//根据输入数据的第一个位置排序，第二个位置求和
        sum.print();

        executionEnvironment.execute();

    }

    //自定义类实现FlatMapper结构
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {
        public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String word = s;
            collector.collect(new Tuple2<String,Integer>(word,1));

        }
    }
}
