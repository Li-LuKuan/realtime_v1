package com.retailersv1.lianxi;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class realtime {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        DataStreamSource<String> streamSource = env.socketTextStream("edh01", 9999);
        streamSource.print();
        env.execute();
    }
}
