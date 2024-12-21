package com.retailersv1.lianxi;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

public class CDC {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("cdh") // monitor all tables under inventory database
                .tableList("cdh.aaa")
//                .databaseList("gmall2023") // monitor all tables under inventory database
//                .tableList("gmall2023.base_trademark")
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> cdcDbDimStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dim_source");

        cdcDbDimStream.print();

//        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
//                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
//                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                        .setTopic("aaa")
//                        .setValueSerializationSchema(new SimpleStringSchema())
//                        .build())
//                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
//                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                //设置事务Id的前缀
//                //.setTransactionalIdPrefix
//                //设置事务的超时时间     检查点超时时间<    事务的超时时间 <=事务最大超时时间
//                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
//                .build();
        cdcDbDimStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092,cdh02:9092,cdh03:9092","aaa"));


        env.execute();
    }
}
