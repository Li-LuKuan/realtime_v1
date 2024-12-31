package com.retailersv1.catalog;

import com.stream.common.utils.ConfigUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class CreateHbaseDImDDLCataLog {
    private static final String HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String ZOOKEEPER_SERVER_HOST_LIST = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String HBASE_CONNECTION_VERSION = "hbase-2.2";

    private static final String createHbaseDimBaseDicDDL = "create table hbase_dim_base_dic (" +
            "   rk string," +
            "   info row<dic_name string, parent_code string>," +
            "   primary key (rk) not enforced" +
            ")" +
            "with (" +
            "   'connector' = '"+ HBASE_CONNECTION_VERSION +"'," +
            "   'table-name' = '"+ HBASE_NAME_SPACE +":dim_base_dic'," +
            "   'zookeeper.quorum' = '"+ ZOOKEEPER_SERVER_HOST_LIST +"'" +
            ")";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = new HiveCatalog("hive-catalog", "default", "/E:/2112AA/stream-dev1/stream-realtime/src/main/resources");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql("show tables;").print();
//        tenv.executeSql(createHbaseDimBaseDicDDL);
        tenv.executeSql("select * from hbase_dim_base_dic").print();
    }
}
