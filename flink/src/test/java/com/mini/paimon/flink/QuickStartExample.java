package com.mini.paimon.flink;

import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.flink.catalog.FlinkCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class QuickStartExample {

    public static void main(String[] args) throws Exception {
        String warehousePath = "./warehouse";
        
        System.out.println("Warehouse path: " + warehousePath);
        
        try {
            EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .build();
            
            TableEnvironment tEnv = TableEnvironment.create(settings);
            
            CatalogContext catalogContext = CatalogContext.builder()
                .warehouse(warehousePath)
                .build();

            FileSystemCatalog paimonCatalog = new FileSystemCatalog("paimon", "default", catalogContext);
            FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", paimonCatalog, "default", warehousePath);
            
            tEnv.registerCatalog("paimon", flinkCatalog);
            tEnv.useCatalog("paimon");
            
            System.out.println("Registered Paimon catalog");

            tEnv.executeSql("CREATE DATABASE IF NOT EXISTS quickstart").await();
            tEnv.executeSql("USE quickstart").await();
            System.out.println("Created and using database: quickstart");

            String createTableSQL = "CREATE TABLE IF NOT EXISTS users (" +
                "user_id BIGINT NOT NULL," +
                "username STRING," +
                "age INT," +
                "email STRING," +
                "PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mini-paimon'," +
                "'warehouse' = '" + warehousePath + "'" +
                ")";
            
            tEnv.executeSql(createTableSQL).await();
            System.out.println("Created table: users");

            String insertSQL = "INSERT INTO users VALUES " +
                "(1, 'alice', 30, 'alice@example.com')," +
                "(2, 'bob', 25, 'bob@example.com')," +
                "(3, 'charlie', 35, 'charlie@example.com')," +
                "(4, 'david', 28, 'david@example.com')," +
                "(5, 'eve', 32, 'eve@example.com')";
            
            TableResult result = tEnv.executeSql(insertSQL);
            result.await();
            System.out.println("Inserted 5 records via Flink SQL");

            System.out.println("QuickStart completed successfully!");
        } finally {
//            Files.walk(tempDir)
//                .sorted(Comparator.reverseOrder())
//                .map(Path::toFile)
//                .forEach(File::delete);
//            System.out.println("Cleaned up temporary directory");
        }
    }
}
