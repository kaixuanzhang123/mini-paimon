package com.mini.paimon.reader;

import com.mini.paimon.metadata.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * List Record Reader
 * 从内存列表中读取记录的简单实现
 */
public class ListRecordReader implements RecordReader<Row> {
    
    private final List<Row> rows;
    private int currentIndex = 0;
    
    public ListRecordReader(List<Row> rows) {
        this.rows = rows != null ? rows : new ArrayList<>();
    }
    
    @Override
    public Row readRecord() throws IOException {
        if (currentIndex < rows.size()) {
            return rows.get(currentIndex++);
        }
        return null;
    }
    
    @Override
    public RecordBatch<Row> readBatch(int maxSize) throws IOException {
        if (currentIndex >= rows.size()) {
            return null;
        }
        
        int batchSize = Math.min(maxSize, rows.size() - currentIndex);
        List<Row> batchRows = new ArrayList<>(batchSize);
        
        for (int i = 0; i < batchSize; i++) {
            batchRows.add(rows.get(currentIndex++));
        }
        
        return new RecordBatch<Row>() {
            @Override
            public int size() {
                return batchRows.size();
            }
            
            @Override
            public Row get(int index) {
                return batchRows.get(index);
            }
        };
    }
    
    @Override
    public void close() throws IOException {
        // Nothing to close
    }
}

