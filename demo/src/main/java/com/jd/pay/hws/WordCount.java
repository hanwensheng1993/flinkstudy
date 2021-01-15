package com.jd.pay.hws;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author hanwensheng
 * @date 2021/1/7
 * WordCount
 */
public class WordCount {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "E:\\develop\\git_study\\flinkstudy\\demo\\src\\main\\resources\\hello.txt";

        DataSet<String> ds = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> result = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 2));
                }
            }
        }).groupBy(0).sum(1);

        result.print();
    }
}
