package com.mini.paimon.spark.source;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.table.Predicate;
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
    private final Predicate predicate;

    public PaimonPartitionReaderFactory(String warehousePath, String database, String tableName, Predicate predicate) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
        this.predicate = predicate;
    }

    @Override
    public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(InputPartition partition) {
        PaimonInputPartition paimonPartition = (PaimonInputPartition) partition;
        
        try {
            CatalogContext context = CatalogContext.builder()
                .warehouse(warehousePath)
                .option("catalog.name", "paimon")
                .option("catalog.default-database", database)
                .build();
            Catalog catalog = com.mini.paimon.catalog.CatalogLoader.load("filesystem", context);
            Identifier identifier = new Identifier(database, tableName);
            Table paimonTable = catalog.getTable(identifier);
            
            if (predicate != null) {
                LOG.info("Created PaimonPartitionReader for {}.{} with filter: {}", database, tableName, predicate);
            } else {
                LOG.info("Created PaimonPartitionReader for {}.{}", database, tableName);
            }
            return new PaimonPartitionReader(paimonTable, predicate);
        } catch (Exception e) {
            LOG.error("Failed to create PaimonPartitionReader", e);
            throw new RuntimeException("Failed to create PaimonPartitionReader", e);
        }
    }
}

