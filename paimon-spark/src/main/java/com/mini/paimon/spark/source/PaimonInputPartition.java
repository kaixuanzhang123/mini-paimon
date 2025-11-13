package com.mini.paimon.spark.source;

import org.apache.spark.sql.connector.read.InputPartition;

import java.io.Serializable;

public class PaimonInputPartition implements InputPartition, Serializable {

    private static final long serialVersionUID = 1L;
    
    // 分区 ID，用于标识不同的分区
    private final int partitionId;

    public PaimonInputPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionId() {
        return partitionId;
    }
}

