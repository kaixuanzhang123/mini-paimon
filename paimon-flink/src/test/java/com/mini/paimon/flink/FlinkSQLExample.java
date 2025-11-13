package com.mini.paimon.flink;

import com.mini.paimon.flink.utils.FlinkEnvironmentFactory;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkSQLExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLExample.class);

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse";
        
        TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(warehousePath);
        
        LOG.info("=== Creating Database ===");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
        tableEnv.executeSql("USE test_db");
        
        LOG.info("=== Creating Table ===");
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS users (" +
            "  id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  email STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'mini-paimon'" +
            ")";
        tableEnv.executeSql(createTableSQL);
        
        LOG.info("=== Inserting Data ===");
        tableEnv.executeSql(
                "INSERT INTO users VALUES " +
                        "(1, 'Alice', 25, 'alice@example.com'), " +
                        "(2, 'Bob', 30, 'bob@example.com'), " +
                        "(3, 'Charlie', 35, 'charlie@example.com')"
        ).await();

        tableEnv.executeSql(
            "INSERT INTO users VALUES " +
            "(4, 'Alice', 25, 'alice@example.com'), " +
            "(5, 'Bob', 30, 'bob@example.com'), " +
            "(6, 'Charlie', 35, 'charlie@example.com')"
        ).await();
        
        LOG.info("=== Querying Data ===");
        TableResult result = tableEnv.executeSql("SELECT * FROM users ORDER BY id");
        result.print();
        
        LOG.info("=== Query with Filter ===");
        result = tableEnv.executeSql("SELECT * FROM users WHERE age > 25");
        result.print();
        
        LOG.info("Flink SQL Example completed successfully!");
    }
}
