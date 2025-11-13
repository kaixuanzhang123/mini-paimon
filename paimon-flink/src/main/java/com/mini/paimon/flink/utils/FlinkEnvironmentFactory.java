package com.mini.paimon.flink.utils;

import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.flink.catalog.FlinkCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkEnvironmentFactory {

    public static TableEnvironment createTableEnvironment(String warehousePath) {
        return createTableEnvironment(warehousePath, "default");
    }

    public static TableEnvironment createTableEnvironment(String warehousePath, String defaultDatabase) {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        CatalogContext catalogContext = CatalogContext.builder()
            .warehouse(warehousePath)
            .build();
        FileSystemCatalog paimonCatalog = new FileSystemCatalog("paimon", warehousePath, catalogContext);
        
        FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", paimonCatalog, defaultDatabase, warehousePath);
        
        tableEnv.registerCatalog("paimon", flinkCatalog);
        tableEnv.useCatalog("paimon");
        
        return tableEnv;
    }
    
    public static TableEnvironment createStreamingTableEnvironment(String warehousePath) {
        return createStreamingTableEnvironment(warehousePath, "default");
    }
    
    public static TableEnvironment createStreamingTableEnvironment(String warehousePath, String defaultDatabase) {
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();
        
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        CatalogContext catalogContext = CatalogContext.builder()
            .warehouse(warehousePath)
            .build();
        FileSystemCatalog paimonCatalog = new FileSystemCatalog("paimon", warehousePath, catalogContext);
        
        FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", paimonCatalog, defaultDatabase, warehousePath);
        
        tableEnv.registerCatalog("paimon", flinkCatalog);
        tableEnv.useCatalog("paimon");
        
        return tableEnv;
    }
}
