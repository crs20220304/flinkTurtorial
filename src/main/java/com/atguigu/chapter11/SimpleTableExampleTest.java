package com.atguigu.chapter11;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SimpleTableExampleTest {
    public static void main(String[] args) {


        // 1. 定义环境配置来创建表
        // 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.useCatalog("my Customer table");
        tableEnv.useDatabase("myCustomerTable");

        String createDDL="CREATE TABLE clickTable" +
                " user_name STRING, " +
                " url STRING, "+
                " ts BIGINT" +
                ")WITH(" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";
        String createOutDDL="CREATE TABLE outTable" +
                " user_name STRING, " +
                " url STRING, "+
                " ts BIGINT" +
                ")WITH(" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv' " +
                ")";


        tableEnv.executeSql(createDDL);
        tableEnv.executeSql(createOutDDL);


    }
}
