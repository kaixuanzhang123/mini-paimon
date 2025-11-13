package com.mini.paimon.spark.source;

import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class PaimonBatch implements Batch {

    private final Table paimonTable;

    public PaimonBatch(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{new PaimonInputPartition(paimonTable)};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new PaimonPartitionReaderFactory(paimonTable);
    }
}

