package com.mini.paimon.spark.source;

import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class PaimonBatch implements Batch {

    private final Table paimonTable;
    private final String warehousePath;
    private final Predicate predicate;

    public PaimonBatch(Table paimonTable, String warehousePath, Predicate predicate) {
        this.paimonTable = paimonTable;
        this.warehousePath = warehousePath;
        this.predicate = predicate;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[]{new PaimonInputPartition(0)};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        String database = paimonTable.identifier().getDatabase();
        String tableName = paimonTable.identifier().getTable();
        return new PaimonPartitionReaderFactory(warehousePath, database, tableName, predicate);
    }
}

