package com.mini.paimon.spark.sink;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.table.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class PaimonDataWriterFactory implements DataWriterFactory, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDataWriterFactory.class);
    private static final long serialVersionUID = 1L;

    private final String warehousePath;
    private final String database;
    private final String tableName;
    private final StructType schema;

    public PaimonDataWriterFactory(String warehousePath, String database, String tableName, StructType schema) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        // 在 worker 节点上重新创建 Table 对象
        try {
            CatalogContext context = CatalogContext.builder()
                .warehouse(warehousePath)
                .build();
            Catalog catalog = new FileSystemCatalog("paimon", "default", context);
            Identifier identifier = new Identifier(database, tableName);
            Table paimonTable = catalog.getTable(identifier);
            
            LOG.info("Created PaimonDataWriter for partition {} task {} on {}.{}", 
                     partitionId, taskId, database, tableName);
            return new PaimonDataWriter(paimonTable, schema, partitionId, taskId);
        } catch (Exception e) {
            LOG.error("Failed to create PaimonDataWriter", e);
            throw new RuntimeException("Failed to create PaimonDataWriter", e);
        }
    }
}

