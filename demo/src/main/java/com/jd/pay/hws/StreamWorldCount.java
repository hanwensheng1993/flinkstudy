package com.jd.pay.hws;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author hanwensheng
 * @date 2021/1/15
 * StreamWorldCount
 * ������
 */
public class StreamWorldCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //���ò����߳���
        env.setParallelism(2);

        /*String inputPath = "E:\\develop\\git_study\\flinkstudy\\demo\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> ds = env.readTextFile(inputPath);*/

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //nc -lk 7777
//        DataStreamSource<String> ds = env.socketTextStream("localhost", 7777);
        DataStreamSource<String> ds = env.socketTextStream(host, port);


        SingleOutputStreamOperator<Tuple2<String, Integer>> result = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 2));
                }
            }
        }).keyBy(0).sum(1);

        result.print();

        env.execute();
    }
}
