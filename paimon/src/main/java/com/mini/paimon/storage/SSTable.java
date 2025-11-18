package com.mini.paimon.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;

import java.io.IOException;
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

    /**
     * Block级索引：定位到数据块
     */
    public static class BlockIndexEntry {
        private final RowKey firstKey;
        private final RowKey lastKey;
        private final long blockOffset;
        private final int blockSize;
        private final int rowCount;
        private final long rowIndexOffset;  // 行级索引的偏移量
        private final int rowIndexSize;     // 行级索引的大小

        @JsonCreator
        public BlockIndexEntry(@JsonProperty("firstKey") RowKey firstKey,
                              @JsonProperty("lastKey") RowKey lastKey,
                              @JsonProperty("blockOffset") long blockOffset,
                              @JsonProperty("blockSize") int blockSize,
                              @JsonProperty("rowCount") int rowCount,
                              @JsonProperty("rowIndexOffset") long rowIndexOffset,
                              @JsonProperty("rowIndexSize") int rowIndexSize) {
            this.firstKey = firstKey;
            this.lastKey = lastKey;
            this.blockOffset = blockOffset;
            this.blockSize = blockSize;
            this.rowCount = rowCount;
            this.rowIndexOffset = rowIndexOffset;
            this.rowIndexSize = rowIndexSize;
        }

        public RowKey getFirstKey() { return firstKey; }
        public RowKey getLastKey() { return lastKey; }
        public long getBlockOffset() { return blockOffset; }
        public int getBlockSize() { return blockSize; }
        public int getRowCount() { return rowCount; }
        public long getRowIndexOffset() { return rowIndexOffset; }
        public int getRowIndexSize() { return rowIndexSize; }
        
        public boolean mightContainKey(RowKey key) {
            return key.compareTo(firstKey) >= 0 && key.compareTo(lastKey) <= 0;
        }

        @Override
        public String toString() {
            return "BlockIndexEntry{" +
                    "firstKey=" + firstKey +
                    ", lastKey=" + lastKey +
                    ", blockOffset=" + blockOffset +
                    ", blockSize=" + blockSize +
                    ", rowCount=" + rowCount +
                    ", rowIndexOffset=" + rowIndexOffset +
                    ", rowIndexSize=" + rowIndexSize +
                    '}';
        }
    }
    
    /**
     * 行级索引：定位到具体某一行数据
     * 参考Paimon设计，索引中包含key、offset、length
     */
    public static class RowIndexEntry {
        private final RowKey key;
        private final int valueOffset;  // 值在valuesData中的偏移量
        private final int valueLength;  // 值的长度

        @JsonCreator
        public RowIndexEntry(@JsonProperty("key") RowKey key,
                            @JsonProperty("valueOffset") int valueOffset,
                            @JsonProperty("valueLength") int valueLength) {
            this.key = key;
            this.valueOffset = valueOffset;
            this.valueLength = valueLength;
        }

        public RowKey getKey() { return key; }
        public int getValueOffset() { return valueOffset; }
        public int getValueLength() { return valueLength; }

        @Override
        public String toString() {
            return "RowIndexEntry{" +
                    "key=" + key +
                    ", valueOffset=" + valueOffset +
                    ", valueLength=" + valueLength +
                    '}';
        }
    }
    
    // 为了向后兼容，保留IndexEntry作为BlockIndexEntry的别名
    @Deprecated
    public static class IndexEntry extends BlockIndexEntry {
        public IndexEntry(RowKey firstKey, RowKey lastKey, long offset, int size, int rowCount) {
            super(firstKey, lastKey, offset, size, rowCount, 0, 0);
        }
        
        public long getOffset() { return getBlockOffset(); }
        public int getSize() { return getBlockSize(); }
    }

    /**
     * DataBlock：实际存储数据
     * 按照Paimon设计，valuesData存储所有行值的连续字节流
     * 通过RowIndexEntry中的offset和length可以精确定位到某一行
     */
    public static class DataBlock {
        private final byte[] valuesData;  // 所有行值的连续字节流
        private final int rowCount;
        
        private transient List<RowIndexEntry> rowIndex;  // 行级索引（延迟加载）

        @JsonCreator
        public DataBlock(@JsonProperty("valuesData") byte[] valuesData,
                        @JsonProperty("rowCount") int rowCount) {
            this.valuesData = valuesData;
            this.rowCount = rowCount;
        }
        
        public byte[] getValuesData() {
            return valuesData;
        }
        
        public int getRowCount() {
            return rowCount;
        }
        
        /**
         * 根据行索引直接读取指定行的数据
         * @param rowIndexEntry 行索引条目
         * @return 行数据
         */
        public Row getRowByIndex(RowIndexEntry rowIndexEntry) throws IOException {
            byte[] rowBytes = new byte[rowIndexEntry.getValueLength()];
            System.arraycopy(valuesData, rowIndexEntry.getValueOffset(), rowBytes, 0, rowIndexEntry.getValueLength());
            
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(rowBytes, Row.class);
        }

        @Override
        public String toString() {
            return "DataBlock{" +
                    "rowCount=" + rowCount +
                    ", dataSize=" + valuesData.length +
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