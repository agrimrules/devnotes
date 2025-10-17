package dev.agrim.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
        public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStream<String> text = env.readTextFile("/opt/flink/usermount/input.txt");

        DataStream<Tuple2<String,Integer>> counts = text
                                                    .flatMap(new Tokenizer())
                                                    .keyBy(v -> v.f0)
                                                    .sum(1);

        counts.print();

        env.execute("Word Count");
        }

   public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line by spaces
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit a (word, 1) pair for each token
            for (String token : tokens) {
                if (token.length() > 0) {
                    System.out.println(token);
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
