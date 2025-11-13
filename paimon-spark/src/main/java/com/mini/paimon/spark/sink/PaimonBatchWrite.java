package com.mini.paimon.spark.sink;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("Committing write with {} messages for {}.{}", messages.length, database, tableName);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("Aborting write with {} messages for {}.{}", messages.length, database, tableName);
    }
}

