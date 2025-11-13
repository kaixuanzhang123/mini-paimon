package com.mini.paimon.spark.sink;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableWrite;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PaimonBatchWrite implements BatchWrite {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonBatchWrite.class);

    private final String warehousePath;
    private final String database;
    private final String tableName;
    private final StructType schema;

    public PaimonBatchWrite(String warehousePath, String database, String tableName, StructType schema) {
        this.warehousePath = warehousePath;
        this.database = database;
        this.tableName = tableName;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new PaimonDataWriterFactory(warehousePath, database, tableName, schema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        try {
            LOG.info("Committing write with {} messages for {}.{}", messages.length, database, tableName);
            
            // 收集所有 writer 的提交消息
            List<ManifestEntry> allFiles = new ArrayList<>();
            long totalRecords = 0;
            int schemaId = 0;
            
            for (WriterCommitMessage message : messages) {
                if (message instanceof PaimonWriterCommitMessage) {
                    PaimonWriterCommitMessage paimonMessage = (PaimonWriterCommitMessage) message;
                    TableWrite.TableCommitMessage commitMessage = paimonMessage.getCommitMessage();
                    allFiles.addAll(commitMessage.getNewFiles());
                    totalRecords += paimonMessage.getRecordCount();
                    schemaId = commitMessage.getSchemaId();
                }
            }
            
            if (allFiles.isEmpty()) {
                LOG.warn("No files to commit for {}.{}", database, tableName);
                return;
            }
            
            // 创建合并后的提交消息
            TableWrite.TableCommitMessage mergedMessage = new TableWrite.TableCommitMessage(
                database, tableName, schemaId,
                System.currentTimeMillis(), // 使用新的 commitIdentifier
                allFiles
            );
            
            // 统一提交所有文件
            CatalogContext context = CatalogContext.builder()
                .warehouse(warehousePath)
                .build();
            Catalog catalog = new FileSystemCatalog("paimon", "default", context);
            Identifier identifier = new Identifier(database, tableName);
            Table paimonTable = catalog.getTable(identifier);
            
            TableCommit tableCommit = paimonTable.newCommit();
            tableCommit.commit(mergedMessage);
            
            LOG.info("Successfully committed {} files with {} total records for {}.{}", 
                    allFiles.size(), totalRecords, database, tableName);
        } catch (Exception e) {
            LOG.error("Failed to commit write for {}.{}", database, tableName, e);
            throw new RuntimeException("Failed to commit write", e);
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("Aborting write with {} messages for {}.{}", messages.length, database, tableName);
    }
}

