package com.retailersv1.util;

import com.alibaba.fastjson.JSONObject;
//import com.atguigu.gamll.realtime.common.bean.TableProcessDwd;
//import com.atguigu.gamll.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class FlinkSinkUtil {
    //获取KafkaSink
    public static KafkaSink<String> getKafkaSink(String topic){
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092，cdh03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //设置事务Id的前缀
                //.setTransactionalIdPrefix
                //设置事务的超时时间     检查点超时时间<    事务的超时时间 <=事务最大超时时间
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
//        dirtyDS.sinkTo(kafkaSink);
        return kafkaSink;
    }

//    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink(){
//        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
//                .setBootstrapServers(Constant.KAFKA_BROKERS)
//                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
//                    @Nullable
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tup2, KafkaSinkContext kafkaSinkContext, Long aLong) {
//                        JSONObject jsonObj = tup2.f0;
//                        TableProcessDwd tableProcessDwd = tup2.f1;
//                        String topic = tableProcessDwd.getSinkTable();
//                        return new ProducerRecord<byte[], byte[]>(topic, jsonObj.toJSONString().getBytes());
//                    }
//                })
//                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
//                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                //设置事务Id的前缀
//                //.setTransactionalIdPrefix
//                //设置事务的超时时间     检查点超时时间<    事务的超时时间 <=事务最大超时时间
//                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
//                .build();
////        dirtyDS.sinkTo(kafkaSink);
//        return kafkaSink;
//    }

//    //扩展： 如果流中数据类型不确定，  如果将数据写到kafka主题
//    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> ksr) {
//        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
//                .setBootstrapServers(Constant.KAFKA_BROKERS)
//                .setRecordSerializer(ksr)
//                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
//                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                //设置事务Id的前缀
//                //.setTransactionalIdPrefix
//                //设置事务的超时时间     检查点超时时间<    事务的超时时间 <=事务最大超时时间
//                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
//                .build();
////        dirtyDS.sinkTo(kafkaSink);
//        return kafkaSink;
//    }

    //获取DorisSink
    public static DorisSink<String> getDorisSink(){
        Properties props =new Properties();
        props.setProperty("format","json");
        props.setProperty("read_json_by_line","true");// 每行一条 json 数据

        DorisSink<String> sink =DorisSink.<String>builder()
                .setDorisExecutionOptions(DorisExecutionOptions.builder().build())
                .setDorisOptions(DorisOptions.builder()// 设置doris 的选按参数
                    .setFenodes("hadoop102:7030")
                    .setTableIdentifier("test.table1")
                    .setUsername("root")
                    .setPassword("aaaaaa")
                    .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                            //.setLabelPrefix("doris-label")    //stream-load 导入的时成的 label 游级
                            .disable2PC()// 开启两阶段提交后,abelPrefix 需装全同唯一,为了测试方便禁用两阶段提交
                            .setDeletable(false)
                            .setBufferCount(3)//用于级fstream load数据的缓冲条数:默认3
                            .setBufferCount(1024*1024)//用于级作stream load数热的级冲区人小:默认1M
                            .setMaxRetries(3)
                            .setStreamLoadProp(props)//&stream load 的数据格式默认起csv,热热需婴改成 ison
                            .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }
}
