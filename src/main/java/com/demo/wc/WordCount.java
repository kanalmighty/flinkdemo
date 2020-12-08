package com.demo.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputPath = "C:\\Users\\Administrator\\Desktop\\文档\\青州项目\\opc服务采集数据\\U_MAK01_DC_DQYSL.txt";
        DataSet<String> inputDataSet = executionEnvironment.readTextFile(inputPath);
        //按照第一个位置分组
        DataSet<Tuple2<String,Integer>> sum = inputDataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1);//根据输入数据的第一个位置排序，第二个位置求和
        sum.print();

    }

    //自定义类实现FlatMapper结构
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
            String word = s;
            collector.collect(new Tuple2<String,Integer>(word,1));

        }
    }
}
