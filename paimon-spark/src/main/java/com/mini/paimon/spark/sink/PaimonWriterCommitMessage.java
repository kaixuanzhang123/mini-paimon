package com.mini.paimon.spark.sink;

import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;

public class PaimonWriterCommitMessage implements WriterCommitMessage, Serializable {

    private final int partitionId;
    private final long recordCount;

    public PaimonWriterCommitMessage(int partitionId, long recordCount) {
        this.partitionId = partitionId;
        this.recordCount = recordCount;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getRecordCount() {
        return recordCount;
    }
}

