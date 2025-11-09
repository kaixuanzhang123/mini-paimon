package com.mini.paimon.reader;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merge Record Reader
 * 合并多个有序 Reader 的记录
 * 参考 Paimon 的 MergeRecordReader 设计
 * 
 * 应用场景：
 * 1. 合并多个 SSTable 文件的数据
 * 2. LSM Tree 的多层数据合并读取
 * 3. 保证读取结果的有序性
 */
public class MergeRecordReader implements RecordReader<Row> {
    private static final Logger logger = LoggerFactory.getLogger(MergeRecordReader.class);
    
    private final Schema schema;
    private final List<FileRecordReader> readers;
    private final PriorityQueue<ReaderWithRow> heap;
    
    public MergeRecordReader(Schema schema, List<FileRecordReader> readers) throws IOException {
        this.schema = schema;
        this.readers = readers;
        this.heap = new PriorityQueue<>(readers.size(), new RowComparator());
        
        // 初始化堆：从每个 Reader 中读取第一条记录
        for (FileRecordReader reader : readers) {
            Row firstRow = reader.readRecord();
            if (firstRow != null) {
                heap.offer(new ReaderWithRow(reader, firstRow));
            }
        }
    }
    
    @Override
    public Row readRecord() throws IOException {
        if (heap.isEmpty()) {
            return null;
        }
        
        // 从堆顶取出最小的记录
        ReaderWithRow min = heap.poll();
        Row result = min.row;
        
        // 从同一个 Reader 中读取下一条记录
        Row nextRow = min.reader.readRecord();
        if (nextRow != null) {
            heap.offer(new ReaderWithRow(min.reader, nextRow));
        }
        
        return result;
    }
    
    @Override
    public RecordBatch<Row> readBatch(int maxRecords) throws IOException {
        List<Row> records = new ArrayList<>(maxRecords);
        
        for (int i = 0; i < maxRecords; i++) {
            Row row = readRecord();
            if (row == null) {
                break;
            }
            records.add(row);
        }
        
        if (records.isEmpty()) {
            return null;
        }
        
        return new SimpleRecordBatch(records);
    }
    
    @Override
    public void close() throws IOException {
        for (FileRecordReader reader : readers) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warn("Failed to close reader: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Reader 和当前行的包装
     */
    private static class ReaderWithRow {
        final FileRecordReader reader;
        final Row row;
        
        ReaderWithRow(FileRecordReader reader, Row row) {
            this.reader = reader;
            this.row = row;
        }
    }
    
    /**
     * 行比较器（基于主键）
     */
    private class RowComparator implements Comparator<ReaderWithRow> {
        @Override
        public int compare(ReaderWithRow o1, ReaderWithRow o2) {
            // TODO: 需要根据 Schema 的主键进行比较
            // 简化实现：比较第一个字段
            if (!schema.hasPrimaryKey()) {
                return 0;
            }
            
            Object[] values1 = o1.row.getValues();
            Object[] values2 = o2.row.getValues();
            
            if (values1.length == 0 || values2.length == 0) {
                return 0;
            }
            
            Object v1 = values1[0];
            Object v2 = values2[0];
            
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
                @SuppressWarnings({"unchecked", "rawtypes"})
                int cmp = ((Comparable) v1).compareTo(v2);
                return cmp;
            }
            
            return 0;
        }
    }
    
    /**
     * 简单的记录批次实现
     */
    private static class SimpleRecordBatch implements RecordBatch<Row> {
        private final List<Row> records;
        
        SimpleRecordBatch(List<Row> records) {
            this.records = records;
        }
        
        @Override
        public int size() {
            return records.size();
        }
        
        @Override
        public Row get(int index) {
            return records.get(index);
        }
    }
}
