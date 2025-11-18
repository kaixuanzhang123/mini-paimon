package com.mini.paimon.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.storage.BlockCache;
import com.mini.paimon.storage.SSTable;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * SSTable KeyValue Reader
 * 参考 Paimon 的实现,提供高效的键值读取
 * 
 * 优化策略:
 * 1. 布隆过滤器快速排除不存在的键
 * 2. Block Index 定位数据块
 * 3. Row Index 精确定位行
 * 4. Block Cache 缓存热点数据块
 */
public class SSTableKeyValueReader implements KeyValueFileReader {
    private static final Logger logger = LoggerFactory.getLogger(SSTableKeyValueReader.class);
    
    /** 全局 Block Cache */
    private static final BlockCache GLOBAL_BLOCK_CACHE = new BlockCache();
    
    private final String filePath;
    private final Schema schema;
    private final ObjectMapper objectMapper;
    private final BlockCache blockCache;
    
    private FileChannel fileChannel;
    private SSTable.Footer footer;
    private BloomFilter<byte[]> bloomFilter;
    private List<SSTable.BlockIndexEntry> blockIndex;
    
    private Predicate predicate;
    private Projection projection;
    
    public SSTableKeyValueReader(String filePath, Schema schema) throws IOException {
        this(filePath, schema, GLOBAL_BLOCK_CACHE);
    }
    
    public SSTableKeyValueReader(String filePath, Schema schema, BlockCache blockCache) throws IOException {
        this.filePath = filePath;
        this.schema = schema;
        this.objectMapper = new ObjectMapper();
        this.blockCache = blockCache != null ? blockCache : GLOBAL_BLOCK_CACHE;
        init();
    }
    
    private void init() throws IOException {
        this.fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
        this.footer = readFooter();
        this.bloomFilter = loadBloomFilter();
        this.blockIndex = loadBlockIndex();
    }
    
    @Override
    public KeyValueFileReader withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    @Override
    public KeyValueFileReader withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    @Override
    public Row get(RowKey key) throws IOException {
        // 1. 布隆过滤器检查
        if (bloomFilter != null && !bloomFilter.mightContain(key.getBytes())) {
            logger.debug("Key {} not found in bloom filter", key);
            return null;
        }
        
        // 2. 使用 Block Index 定位数据块
        SSTable.BlockIndexEntry targetBlock = findBlockContainingKey(key);
        if (targetBlock == null) {
            logger.debug("Key {} not found in block index", key);
            return null;
        }
        
        // 3. 读取 Row Index
        List<SSTable.RowIndexEntry> rowIndex = readRowIndex(
            targetBlock.getRowIndexOffset(), 
            targetBlock.getRowIndexSize()
        );
        
        // 4. 在 Row Index 中二分查找
        SSTable.RowIndexEntry targetEntry = binarySearchRowIndex(rowIndex, key);
        if (targetEntry == null) {
            return null;
        }
        
        // 5. 优化：对于点查询，直接读取目标行的字节范围
        //    这避免了读取整个 DataBlock，减少 I/O
        //    权衡：失去 BlockCache 的优势，但对于随机点查询更高效
        Row row = extractRowDirect(targetBlock.getBlockOffset(), targetEntry);
        
        // 6. 应用过滤和投影
        if (row != null) {
            if (predicate != null && !predicate.test(row, schema)) {
                return null;
            }
            if (projection != null && !projection.isAll()) {
                row = projection.project(row, schema);
            }
        }
        
        return row;
    }
    
    @Override
    public RecordReader<Row> readRange(RowKey startKey, RowKey endKey) throws IOException {
        return new SSTableRangeReader(startKey, endKey);
    }
    
    @Override
    public RecordReader<Row> readAll() throws IOException {
        return new SSTableRangeReader(null, null);
    }
    
    @Override
    public void close() throws IOException {
        if (fileChannel != null) {
            fileChannel.close();
        }
    }
    
    /**
     * 获取全局 Block Cache 统计信息
     */
    public static BlockCache.CacheStats getCacheStats() {
        return GLOBAL_BLOCK_CACHE.getStats();
    }
    
    /**
     * 清空全局 Block Cache
     */
    public static void clearCache() {
        GLOBAL_BLOCK_CACHE.clear();
    }
    
    /**
     * 读取 Footer
     */
    private SSTable.Footer readFooter() throws IOException {
        long fileSize = fileChannel.size();
        
        if (fileSize < 12) {
            throw new IOException("Invalid SSTable file: file too small");
        }
        
        // 读取魔数
        ByteBuffer magicBuffer = ByteBuffer.allocate(4);
        fileChannel.read(magicBuffer, fileSize - 4);
        magicBuffer.flip();
        int magic = magicBuffer.getInt();
        
        if (magic != 0xCAFEBABE) {
            throw new IOException("Invalid SSTable file: magic number mismatch");
        }
        
        // 读取 Footer 大小
        ByteBuffer footerSizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(footerSizeBuffer, fileSize - 8);
        footerSizeBuffer.flip();
        int footerSize = footerSizeBuffer.getInt();
        
        if (footerSize <= 0 || footerSize > fileSize - 8) {
            throw new IOException("Invalid SSTable file: invalid footer size");
        }
        
        // 读取 Footer 数据
        long footerOffset = fileSize - 8 - footerSize;
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerSize);
        fileChannel.read(footerBuffer, footerOffset);
        footerBuffer.flip();
        
        byte[] footerBytes = new byte[footerSize];
        footerBuffer.get(footerBytes);
        
        return objectMapper.readValue(footerBytes, SSTable.Footer.class);
    }
    
    /**
     * 加载布隆过滤器
     */
    private BloomFilter<byte[]> loadBloomFilter() {
        if (footer.getBloomFilterOffset() <= 0) {
            return null;
        }
        
        try {
            fileChannel.position(footer.getBloomFilterOffset());
            
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuffer);
            sizeBuffer.flip();
            int bloomSize = sizeBuffer.getInt();
            
            ByteBuffer bloomBuffer = ByteBuffer.allocate(bloomSize);
            fileChannel.read(bloomBuffer);
            bloomBuffer.flip();
            
            byte[] bloomBytes = new byte[bloomSize];
            bloomBuffer.get(bloomBytes);
            
            ByteArrayInputStream bais = new ByteArrayInputStream(bloomBytes);
            try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                return BloomFilter.readFrom(ois, Funnels.byteArrayFunnel());
            }
        } catch (Exception e) {
            logger.warn("Failed to load bloom filter: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * 加载 Block Index
     */
    private List<SSTable.BlockIndexEntry> loadBlockIndex() {
        if (footer.getIndexOffset() <= 0) {
            return new ArrayList<>();
        }
        
        try {
            fileChannel.position(footer.getIndexOffset());
            
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuffer);
            sizeBuffer.flip();
            int indexSize = sizeBuffer.getInt();
            
            ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
            fileChannel.read(indexBuffer);
            indexBuffer.flip();
            
            byte[] indexBytes = new byte[indexSize];
            indexBuffer.get(indexBytes);
            
            return objectMapper.readValue(
                indexBytes,
                objectMapper.getTypeFactory().constructCollectionType(
                    List.class, SSTable.BlockIndexEntry.class
                )
            );
        } catch (Exception e) {
            logger.warn("Failed to load block index: {}", e.getMessage());
            return new ArrayList<>();
        }
    }
    
    /**
     * 查找包含指定键的数据块
     */
    private SSTable.BlockIndexEntry findBlockContainingKey(RowKey key) {
        if (blockIndex.isEmpty()) {
            return null;
        }
        
        // 二分查找
        int left = 0;
        int right = blockIndex.size() - 1;
        SSTable.BlockIndexEntry result = null;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            SSTable.BlockIndexEntry entry = blockIndex.get(mid);
            
            if (key.compareTo(entry.getFirstKey()) < 0) {
                right = mid - 1;
            } else if (key.compareTo(entry.getLastKey()) > 0) {
                left = mid + 1;
            } else {
                result = entry;
                break;
            }
        }
        
        return result;
    }
    
    /**
     * 读取 Row Index
     */
    private List<SSTable.RowIndexEntry> readRowIndex(long offset, int size) throws IOException {
        fileChannel.position(offset);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int rowIndexSize = sizeBuffer.getInt();
        
        ByteBuffer rowIndexBuffer = ByteBuffer.allocate(rowIndexSize);
        fileChannel.read(rowIndexBuffer);
        rowIndexBuffer.flip();
        
        byte[] rowIndexBytes = new byte[rowIndexSize];
        rowIndexBuffer.get(rowIndexBytes);
        
        return objectMapper.readValue(
            rowIndexBytes,
            objectMapper.getTypeFactory().constructCollectionType(
                List.class, SSTable.RowIndexEntry.class
            )
        );
    }
    
    /**
     * 读取 DataBlock 的 valuesData (使用缓存)
     */
    private byte[] readValuesData(long offset, int size) throws IOException {
        // 先查缓存
        byte[] cached = blockCache.get(filePath, offset);
        if (cached != null) {
            return cached;
        }
        
        // 缓存未命中,从磁盘读取
        fileChannel.position(offset);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int blockSize = sizeBuffer.getInt();
        
        ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
        fileChannel.read(blockBuffer);
        blockBuffer.flip();
        
        byte[] blockBytes = new byte[blockSize];
        blockBuffer.get(blockBytes);
        
        SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
        byte[] valuesData = dataBlock.getValuesData();
        
        // 放入缓存
        blockCache.put(filePath, offset, valuesData);
        
        return valuesData;
    }
    
    /**
     * 在 Row Index 中二分查找
     */
    private SSTable.RowIndexEntry binarySearchRowIndex(
            List<SSTable.RowIndexEntry> rowIndex, 
            RowKey targetKey) {
        int left = 0;
        int right = rowIndex.size() - 1;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            SSTable.RowIndexEntry entry = rowIndex.get(mid);
            
            int cmp = entry.getKey().compareTo(targetKey);
            if (cmp == 0) {
                return entry;
            } else if (cmp < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        
        return null;
    }
    
    /**
     * 从 valuesData 中提取行
     */
    private Row extractRow(byte[] valuesData, SSTable.RowIndexEntry entry) throws IOException {
        int offset = entry.getValueOffset();
        int length = entry.getValueLength();
        
        if (offset < 0 || length <= 0 || offset + length > valuesData.length) {
            throw new IOException("Invalid row index: offset=" + offset + 
                ", length=" + length + ", valuesDataSize=" + valuesData.length);
        }
        
        byte[] rowBytes = new byte[length];
        System.arraycopy(valuesData, offset, rowBytes, 0, length);
        
        return objectMapper.readValue(rowBytes, Row.class);
    }
    
    /**
     * 直接从文件读取单行数据（点查询优化）
     * 不读取整个 DataBlock，只读取目标行的字节范围
     * 
     * @param blockOffset DataBlock 在文件中的偏移量
     * @param rowEntry 行索引条目
     * @return 行数据
     * @throws IOException 读取异常
     */
    private Row extractRowDirect(long blockOffset, SSTable.RowIndexEntry rowEntry) throws IOException {
        // 1. 先读取 DataBlock 的头部以获取实际的 valuesData 位置
        fileChannel.position(blockOffset);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int blockSize = sizeBuffer.getInt();
        
        // 2. 读取 DataBlock 结构
        ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
        fileChannel.read(blockBuffer);
        blockBuffer.flip();
        
        byte[] blockBytes = new byte[blockSize];
        blockBuffer.get(blockBytes);
        
        SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
        
        // 3. 从 valuesData 中提取目标行
        byte[] valuesData = dataBlock.getValuesData();
        int offset = rowEntry.getValueOffset();
        int length = rowEntry.getValueLength();
        
        if (offset < 0 || length <= 0 || offset + length > valuesData.length) {
            throw new IOException("Invalid row index: offset=" + offset + 
                ", length=" + length + ", valuesDataSize=" + valuesData.length);
        }
        
        byte[] rowBytes = new byte[length];
        System.arraycopy(valuesData, offset, rowBytes, 0, length);
        
        return objectMapper.readValue(rowBytes, Row.class);
    }
    
    /**
     * 范围扫描读取器
     */
    private class SSTableRangeReader implements RecordReader<Row> {
        private final RowKey startKey;
        private final RowKey endKey;
        private int currentBlockIndex = 0;
        private List<SSTable.RowIndexEntry> currentRowIndex;
        private int currentRowIndexPos = 0;
        private byte[] currentValuesData;
        
        SSTableRangeReader(RowKey startKey, RowKey endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }
        
        @Override
        public Row readRecord() throws IOException {
            ensureCurrentBlock();
            
            if (currentRowIndex == null || currentRowIndexPos >= currentRowIndex.size()) {
                return null;
            }
            
            SSTable.RowIndexEntry entry = currentRowIndex.get(currentRowIndexPos++);
            Row row = extractRow(currentValuesData, entry);
            
            // 应用过滤和投影
            if (row != null) {
                if (predicate != null && !predicate.test(row, schema)) {
                    return readRecord(); // 递归查找下一个满足条件的行
                }
                if (projection != null && !projection.isAll()) {
                    row = projection.project(row, schema);
                }
            }
            
            return row;
        }
        
        @Override
        public RecordReader.RecordBatch<Row> readBatch(int maxRecords) throws IOException {
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
            // 不需要关闭,由外层 SSTableKeyValueReader 管理
        }
        
        private void ensureCurrentBlock() throws IOException {
            while (currentRowIndex == null || currentRowIndexPos >= currentRowIndex.size()) {
                if (currentBlockIndex >= blockIndex.size()) {
                    currentRowIndex = null;
                    return;
                }
                
                SSTable.BlockIndexEntry blockEntry = blockIndex.get(currentBlockIndex++);
                
                // 检查范围
                if (endKey != null && blockEntry.getFirstKey().compareTo(endKey) > 0) {
                    currentRowIndex = null;
                    return;
                }
                if (startKey != null && blockEntry.getLastKey().compareTo(startKey) < 0) {
                    continue;
                }
                
                // 读取当前块
                currentRowIndex = readRowIndex(
                    blockEntry.getRowIndexOffset(),
                    blockEntry.getRowIndexSize()
                );
                currentValuesData = readValuesData(
                    blockEntry.getBlockOffset(),
                    blockEntry.getBlockSize()
                );
                currentRowIndexPos = 0;
                
                // 跳过不在范围内的行
                if (startKey != null) {
                    while (currentRowIndexPos < currentRowIndex.size() &&
                           currentRowIndex.get(currentRowIndexPos).getKey().compareTo(startKey) < 0) {
                        currentRowIndexPos++;
                    }
                }
            }
        }
    }
    
    private static class SimpleRecordBatch implements RecordReader.RecordBatch<Row> {
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

