package com.retailersv1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class TestFlinkSQLAPI {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "/E:/2112AA/stream-dev1/stream-realtime/src/main/resources/");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");


        tenv.executeSql("select rk,\n" +
                "       info.dic_name as dic_name,\n" +
                "       info.parent_code as parent_code\n" +
                "from hbase_dim_base_dic").print();
    }
}
