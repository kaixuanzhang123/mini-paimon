package com.mini.paimon.flink.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableWrite;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkTableWriter {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkTableWriter.class);
    
    private final Table table;
    private final TableWrite tableWrite;

    public FlinkTableWriter(Table table) {
        this.table = table;
        this.tableWrite = table.newWrite();
    }

    public void write(RowData rowData) throws Exception {
        if (rowData.getRowKind() == RowKind.INSERT || rowData.getRowKind() == RowKind.UPDATE_AFTER) {
            Row row = FlinkRowConverter.toRow(rowData, table.schema());
            tableWrite.write(row);
        } else if (rowData.getRowKind() == RowKind.DELETE || rowData.getRowKind() == RowKind.UPDATE_BEFORE) {
            // 暂不支持删除操作
            throw new UnsupportedOperationException("Delete operation is not supported yet");
        }
    }

    public void flush() throws Exception {
        tableWrite.prepareCommit();
    }

    public void commit() throws Exception {
        table.newCommit().commit(tableWrite.prepareCommit());
    }

    public void close() throws Exception {
        if (tableWrite != null) {
            tableWrite.close();
        }
    }
}
