package com.mini.paimon.spark.sink;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.spark.table.SparkRowConverter;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableWrite;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PaimonDataWriter implements DataWriter<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonDataWriter.class);

    private final Table paimonTable;
    private final StructType schema;
    private final int partitionId;
    private final long taskId;
    private final TableWrite tableWrite;
    private long recordCount = 0;

    public PaimonDataWriter(Table paimonTable, StructType schema, int partitionId, long taskId) {
        this.paimonTable = paimonTable;
        this.schema = schema;
        this.partitionId = partitionId;
        this.taskId = taskId;
        
        // 使用 taskId 作为唯一的 writer ID 来避免 WAL 文件冲突
        this.tableWrite = paimonTable.newWrite(taskId);
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            Row paimonRow = SparkRowConverter.toPaimonRow(record, schema, paimonTable.schema());
            tableWrite.write(paimonRow);
            recordCount++;
        } catch (Exception e) {
            LOG.error("Failed to write record", e);
            throw new IOException("Failed to write record", e);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            // 只准备提交，不实际提交。所有 writer 的提交消息会在 BatchWrite.commit() 中统一提交
            TableWrite.TableCommitMessage commitMessage = tableWrite.prepareCommit();
            tableWrite.close();
            
            LOG.info("Prepared {} records from partition {} task {} for commit", recordCount, partitionId, taskId);
            // 将提交消息包装到 WriterCommitMessage 中，传递给 BatchWrite
            return new PaimonWriterCommitMessage(partitionId, recordCount, commitMessage);
        } catch (Exception e) {
            LOG.error("Failed to prepare commit", e);
            throw new IOException("Failed to prepare commit", e);
        }
    }

    @Override
    public void abort() throws IOException {
        try {
            tableWrite.close();
            LOG.warn("Aborted write for partition {} task {}", partitionId, taskId);
        } catch (Exception e) {
            LOG.error("Failed to abort", e);
            throw new IOException("Failed to abort", e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}

