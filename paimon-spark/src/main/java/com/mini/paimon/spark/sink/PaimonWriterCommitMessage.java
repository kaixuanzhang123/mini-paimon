package com.mini.paimon.spark.sink;

import com.mini.paimon.table.TableWrite;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.Serializable;

public class PaimonWriterCommitMessage implements WriterCommitMessage, Serializable {

    private static final long serialVersionUID = 1L;

    private final int partitionId;
    private final long recordCount;
    private final TableWrite.TableCommitMessage commitMessage;

    public PaimonWriterCommitMessage(int partitionId, long recordCount, TableWrite.TableCommitMessage commitMessage) {
        this.partitionId = partitionId;
        this.recordCount = recordCount;
        this.commitMessage = commitMessage;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getRecordCount() {
        return recordCount;
    }
    
    public TableWrite.TableCommitMessage getCommitMessage() {
        return commitMessage;
    }
}
