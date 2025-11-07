package com.mini.paimon.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;

import java.util.List;

/**
 * SSTable 文件结构定义
 * 包含文件元信息和数据结构
 */
public class SSTable {
    
    /** SSTable 文件元信息 */
    public static class Footer {
        /** 索引块在文件中的偏移量 */
        private final long indexOffset;
        
        /** 索引块大小 */
        private final long indexSize;
        
        /** 布隆过滤器在文件中的偏移量 */
        private final long bloomFilterOffset;
        
        /** 布隆过滤器大小 */
        private final long bloomFilterSize;
        
        /** 数据块数量 */
        private final int dataBlockCount;
        
        /** 总行数 */
        private final long rowCount;
        
        /** 最小主键 */
        private final RowKey minKey;
        
        /** 最大主键 */
        private final RowKey maxKey;

        @JsonCreator
        public Footer(@JsonProperty("indexOffset") long indexOffset, 
                     @JsonProperty("indexSize") long indexSize, 
                     @JsonProperty("bloomFilterOffset") long bloomFilterOffset, 
                     @JsonProperty("bloomFilterSize") long bloomFilterSize, 
                     @JsonProperty("dataBlockCount") int dataBlockCount, 
                     @JsonProperty("rowCount") long rowCount,
                     @JsonProperty("minKey") RowKey minKey, 
                     @JsonProperty("maxKey") RowKey maxKey) {
            this.indexOffset = indexOffset;
            this.indexSize = indexSize;
            this.bloomFilterOffset = bloomFilterOffset;
            this.bloomFilterSize = bloomFilterSize;
            this.dataBlockCount = dataBlockCount;
            this.rowCount = rowCount;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }

        // Getters
        public long getIndexOffset() { return indexOffset; }
        public long getIndexSize() { return indexSize; }
        public long getBloomFilterOffset() { return bloomFilterOffset; }
        public long getBloomFilterSize() { return bloomFilterSize; }
        public int getDataBlockCount() { return dataBlockCount; }
        public long getRowCount() { return rowCount; }
        public RowKey getMinKey() { return minKey; }
        public RowKey getMaxKey() { return maxKey; }

        @Override
        public String toString() {
            return "Footer{" +
                    "indexOffset=" + indexOffset +
                    ", indexSize=" + indexSize +
                    ", bloomFilterOffset=" + bloomFilterOffset +
                    ", bloomFilterSize=" + bloomFilterSize +
                    ", dataBlockCount=" + dataBlockCount +
                    ", rowCount=" + rowCount +
                    ", minKey=" + minKey +
                    ", maxKey=" + maxKey +
                    '}';
        }
    }

    /** 索引条目 */
    public static class IndexEntry {
        /** 主键 */
        private final RowKey key;
        
        /** 数据块在文件中的偏移量 */
        private final long offset;

        @JsonCreator
        public IndexEntry(@JsonProperty("key") RowKey key, 
                         @JsonProperty("offset") long offset) {
            this.key = key;
            this.offset = offset;
        }

        public RowKey getKey() { return key; }
        public long getOffset() { return offset; }

        @Override
        public String toString() {
            return "IndexEntry{" +
                    "key=" + key +
                    ", offset=" + offset +
                    '}';
        }
    }

    /** 数据块 */
    public static class DataBlock {
        /** 数据块中的行数据 */
        private final List<RowData> rows;

        @JsonCreator
        public DataBlock(@JsonProperty("rows") List<RowData> rows) {
            this.rows = rows;
        }

        public List<RowData> getRows() {
            return rows;
        }

        @Override
        public String toString() {
            return "DataBlock{" +
                    "rowsCount=" + rows.size() +
                    '}';
        }
    }

    /** 行数据包装类 */
    public static class RowData {
        /** 主键 */
        private final RowKey key;
        
        /** 行数据 */
        private final Row row;

        @JsonCreator
        public RowData(@JsonProperty("key") RowKey key, 
                      @JsonProperty("row") Row row) {
            this.key = key;
            this.row = row;
        }

        public RowKey getKey() { return key; }
        public Row getRow() { return row; }

        @Override
        public String toString() {
            return "RowData{" +
                    "key=" + key +
                    ", row=" + row +
                    '}';
        }
    }
}