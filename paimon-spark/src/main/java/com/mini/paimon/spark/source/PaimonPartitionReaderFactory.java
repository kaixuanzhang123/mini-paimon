package com.mini.paimon.spark.source;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class PaimonPartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionReaderFactory.class);
    private static final long serialVersionUID = 1L;

    private final String warehousePath;
    private final String database;
    private final String tableName;

    public PaimonPartitionReaderFactory(String warehousePath, String database, String tableName) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
    }

    @Override
    public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(InputPartition partition) {
        PaimonInputPartition paimonPartition = (PaimonInputPartition) partition;
        
        // 在 worker 节点上重新创建 Table 对象
        try {
            CatalogContext context = CatalogContext.builder()
                .warehouse(warehousePath)
                .build();
            Catalog catalog = new FileSystemCatalog("paimon", "default", context);
            Identifier identifier = new Identifier(database, tableName);
            Table paimonTable = catalog.getTable(identifier);
            
            LOG.info("Created PaimonPartitionReader for {}.{}", database, tableName);
            return new PaimonPartitionReader(paimonTable);
        } catch (Exception e) {
            LOG.error("Failed to create PaimonPartitionReader", e);
            throw new RuntimeException("Failed to create PaimonPartitionReader", e);
        }
    }
}

