package com.retailersv1.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.CommonUtils;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Date;

public class DwdInteractionCommentInfo {
    private static final String kafka_topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    @SneakyThrows
    public static void main(String[] args) {
//        CommonUtils.printCheckPropEnv(
//                false,
//                kafka_topic_db,
//                kafka_botstrap_servers
//        );

//        DataStreamSource<String> kafkaSourceDs = env.fromSource(
//                KafkaUtils.buildKafkaSource(
//                        kafka_botstrap_servers,
//                        kafka_topic_db,
//                        new Date().toString(),
//                        OffsetsInitializer.earliest()
//                ),
//                WatermarkStrategy.noWatermarks(),
//                "read_kafka_realtime_log"
//        );

//        SingleOutputStreamOperator<JSONObject> processDS = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
//                    @Override
//                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
//                        try {
//                            collector.collect(JSONObject.parseObject(s));
//                        } catch (Exception e) {
//                            System.err.println("Convert JsonData Error !");
//                        }
//                    }
//                }).uid("convert_json_process")
//                .name("convert_json_process");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql( "CREATE TABLE topic_db (\n" +
                "    `before` STRING,\n" +
                "    `after` STRING,\n" +
                "    `source` STRING,\n" +
                "    `op` STRING,\n" +
                "    `ts_ms` BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+kafka_topic_db+"',\n" +
                "    'properties.bootstrap.servers' = '"+kafka_botstrap_servers+"',\n" +
                "    'properties.group.id' = 'dwd_interaction_comment_info',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from topic_db").print();
        Table table = tableEnv.sqlQuery("select * from topic_db");
        DataStream<Row> data = tableEnv.toDataStream(table);

        data.print();


        env.execute();
    }
}
