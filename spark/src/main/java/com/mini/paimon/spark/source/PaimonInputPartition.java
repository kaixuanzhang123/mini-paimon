package com.mini.paimon.spark.source;

import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.read.InputPartition;

public class PaimonInputPartition implements InputPartition {

    private final Table paimonTable;

    public PaimonInputPartition(Table paimonTable) {
        this.paimonTable = paimonTable;
    }

    public Table getPaimonTable() {
        return paimonTable;
    }
}

