package com.mini.paimon.spark.sink;

import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaimonBatchWrite implements BatchWrite {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonBatchWrite.class);

    private final Table paimonTable;
    private final StructType schema;

    public PaimonBatchWrite(Table paimonTable, StructType schema) {
        this.paimonTable = paimonTable;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new PaimonDataWriterFactory(paimonTable, schema);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        LOG.info("Committing write with {} messages", messages.length);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        LOG.warn("Aborting write with {} messages", messages.length);
    }
}

