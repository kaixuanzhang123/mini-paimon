package com.mini.paimon.spark.source;

import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class PaimonPartitionReaderFactory implements PartitionReaderFactory {

    private final Table paimonTable;

    public PaimonPartitionReaderFactory(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    @Override
    public PartitionReader<org.apache.spark.sql.catalyst.InternalRow> createReader(InputPartition partition) {
        PaimonInputPartition paimonPartition = (PaimonInputPartition) partition;
        return new PaimonPartitionReader(paimonPartition.getPaimonTable());
    }
}

