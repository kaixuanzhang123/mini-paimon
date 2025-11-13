package com.mini.paimon.spark.sink;

import com.mini.paimon.table.Table;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class PaimonDataWriterFactory implements DataWriterFactory {

    private final Table paimonTable;
    private final StructType schema;

    public PaimonDataWriterFactory(Table paimonTable, StructType schema) {
        this.paimonTable = paimonTable;
        this.schema = schema;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new PaimonDataWriter(paimonTable, schema, partitionId, taskId);
    }
}

