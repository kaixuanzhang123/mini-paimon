package com.mini.paimon.reader;

import com.mini.paimon.index.SimpleBitmap;
import com.mini.paimon.metadata.Row;

import java.io.IOException;

/**
 * BitmapIndexRecordReader - 行级过滤读取器
 * 包装现有的RecordReader，使用Bitmap索引跳过不匹配的行
 * 参考Paimon的ApplyBitmapIndexRecordReader实现
 */
public class BitmapIndexRecordReader implements RecordReader<Row> {
    
    /** 底层的RecordReader */
    private final RecordReader<Row> reader;
    
    /** 需要返回的行号位图 */
    private final SimpleBitmap bitmap;
    
    /** 当前读取的行号 */
    private int currentRowNumber;
    
    /**
     * 构造函数
     * @param reader 底层的RecordReader
     * @param bitmap 需要返回的行号位图
     */
    public BitmapIndexRecordReader(RecordReader<Row> reader, SimpleBitmap bitmap) {
        this.reader = reader;
        this.bitmap = bitmap;
        this.currentRowNumber = 0;
    }
    
    @Override
    public Row readRecord() throws IOException {
        while (true) {
            Row row = reader.readRecord();
            
            // 如果没有更多数据，返回null
            if (row == null) {
                return null;
            }
            
            int rowNumber = currentRowNumber++;
            
            // 如果当前行在位图中，返回该行
            if (bitmap.contains(rowNumber)) {
                return row;
            }
            
            // 否则跳过该行，继续读取下一行
        }
    }
    
    @Override
    public RecordBatch<Row> readBatch(int maxRecords) throws IOException {
        // 简单实现：逐行读取，直到达到maxRecords或文件结束
        java.util.List<Row> rows = new java.util.ArrayList<>();
        
        for (int i = 0; i < maxRecords; i++) {
            Row row = readRecord();
            if (row == null) {
                break;
            }
            rows.add(row);
        }
        
        if (rows.isEmpty()) {
            return null;
        }
        
        return new SimpleRecordBatch(rows);
    }
    
    @Override
    public void close() throws IOException {
        reader.close();
    }
    
    /**
     * 简单的RecordBatch实现
     */
    private static class SimpleRecordBatch implements RecordBatch<Row> {
        private final java.util.List<Row> rows;
        
        SimpleRecordBatch(java.util.List<Row> rows) {
            this.rows = rows;
        }
        
        @Override
        public int size() {
            return rows.size();
        }
        
        @Override
        public Row get(int index) {
            return rows.get(index);
        }
    }
}

