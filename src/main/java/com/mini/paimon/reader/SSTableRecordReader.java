package com.mini.paimon.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.storage.SSTable;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SSTable Record Reader
 * 基于 Block 的 SSTable 记录读取器
 * 参考 Paimon 的实现，支持 Block 级别的优化
 * 
 * 优化策略：
 * 1. Block 级别的懒加载 - 只读取需要的 Block
 * 2. 谓词下推到 Block 级别 - 跳过不满足条件的 Block
 * 3. 投影下推 - 只读取需要的字段（未来优化）
 */
public class SSTableRecordReader implements FileRecordReader {
    private static final Logger logger = LoggerFactory.getLogger(SSTableRecordReader.class);
    
    private final String filePath;
    private final Schema schema;
    private final ObjectMapper objectMapper;
    
    private FileChannel fileChannel;
    private SSTable.Footer footer;
    
    // 过滤和投影
    private Predicate predicate;
    private Projection projection;
    
    private Iterator<BlockInfo> blockIterator;
    private Iterator<Row> currentBlockRows;
    
    private RowKey startKey;
    private RowKey endKey;
    
    private boolean isPointQuery = false;
    private RowKey exactKey = null;
    private boolean initialized = false;
    
    public SSTableRecordReader(String filePath, Schema schema) throws IOException {
        this.filePath = filePath;
        this.schema = schema;
        this.objectMapper = new ObjectMapper();
    }
    
    public SSTableRecordReader withFilter(Predicate predicate) {
        this.predicate = predicate;
        if (!isInitialized()) {
            extractKeyFromPredicate(predicate);
        }
        return this;
    }
    
    /**
     * 设置投影
     */
    public SSTableRecordReader withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    public SSTableRecordReader withRange(RowKey startKey, RowKey endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.isPointQuery = (startKey != null && endKey != null && startKey.equals(endKey));
        if (isPointQuery) {
            this.exactKey = startKey;
        }
        return this;
    }
    
    private void extractKeyFromPredicate(Predicate pred) {
        if (pred == null || !schema.hasPrimaryKey()) {
            return;
        }
        
        if (pred instanceof Predicate.FieldPredicate) {
            Predicate.FieldPredicate fp = (Predicate.FieldPredicate) pred;
            if (fp.getOp() == Predicate.CompareOp.EQ && isPrimaryKeyField(fp.getFieldName())) {
                this.exactKey = createRowKeyFromValue(fp.getValue());
                this.isPointQuery = true;
                this.startKey = exactKey;
                this.endKey = exactKey;
            }
        }
    }
    
    private boolean isPrimaryKeyField(String fieldName) {
        List<String> primaryKeys = schema.getPrimaryKeys();
        return !primaryKeys.isEmpty() && primaryKeys.get(0).equals(fieldName);
    }
    
    private RowKey createRowKeyFromValue(Object value) {
        if (value instanceof Integer) {
            return new RowKey(java.nio.ByteBuffer.allocate(4).putInt((Integer) value).array());
        } else if (value instanceof Long) {
            return new RowKey(java.nio.ByteBuffer.allocate(8).putLong((Long) value).array());
        } else if (value instanceof String) {
            return new RowKey(((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return null;
    }
    
    private void init() throws IOException {
        if (initialized) {
            return;
        }
        
        Path path = Paths.get(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.footer = readFooter();
        
        if (isPointQuery && exactKey != null) {
            this.blockIterator = buildPointQueryIterator(exactKey);
        } else {
            this.blockIterator = buildBlockIterator();
        }
        this.currentBlockRows = null;
        this.initialized = true;
    }
    
    private boolean isInitialized() {
        return initialized;
    }
    
    @Override
    public String getFilePath() {
        return filePath;
    }
    
    @Override
    public void seekToKey(RowKey key) throws IOException {
        this.startKey = key;
        this.initialized = false;
        this.blockIterator = null;
        this.currentBlockRows = null;
    }
    
    @Override
    public boolean hasNext() throws IOException {
        init();
        ensureCurrentBlockRows();
        return currentBlockRows != null && currentBlockRows.hasNext();
    }
    
    @Override
    public Row readRecord() throws IOException {
        init();
        ensureCurrentBlockRows();
        
        if (currentBlockRows == null || !currentBlockRows.hasNext()) {
            return null;
        }
        
        return currentBlockRows.next();
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
        if (fileChannel != null) {
            fileChannel.close();
        }
    }
    
    private void ensureCurrentBlockRows() throws IOException {
        while ((currentBlockRows == null || !currentBlockRows.hasNext()) && blockIterator.hasNext()) {
            BlockInfo blockInfo = blockIterator.next();
            
            if (canSkipBlock(blockInfo)) {
                logger.debug("Skipped block at offset {}", blockInfo.offset);
                continue;
            }
            
            List<Row> rows = readBlockRows(blockInfo);
            
            if (predicate != null && !isPointQuery) {
                rows = applyFilter(rows);
            }
            
            if (projection != null) {
                rows = applyProjection(rows);
            }
            
            currentBlockRows = rows.iterator();
        }
    }
    
    private boolean canSkipBlock(BlockInfo blockInfo) {
        if (blockInfo.minKey == null || blockInfo.maxKey == null) {
            return false;
        }
        
        if (startKey != null && blockInfo.maxKey.compareTo(startKey) < 0) {
            return true;
        }
        if (endKey != null && blockInfo.minKey.compareTo(endKey) > 0) {
            return true;
        }
        
        return false;
    }
    
    /**
     * 读取Block中的数据行
     * 优化逻辑：
     * 1. 先读取RowIndex
     * 2. 通过RowIndex中的key进行二分查找
     * 3. 找到后直接通过offset和length读取指定行数据
     */
    private List<Row> readBlockRows(BlockInfo blockInfo) throws IOException {
        List<Row> rows = new ArrayList<>();
        
        try {
            // 1. 读取DataBlock
            fileChannel.position(blockInfo.offset);
            
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
            
            // 2. 读取RowIndex（紧跟在DataBlock后）
            ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(rowIndexSizeBuffer);
            rowIndexSizeBuffer.flip();
            int rowIndexSize = rowIndexSizeBuffer.getInt();
            
            ByteBuffer rowIndexBuffer = ByteBuffer.allocate(rowIndexSize);
            fileChannel.read(rowIndexBuffer);
            rowIndexBuffer.flip();
            
            byte[] rowIndexBytes = new byte[rowIndexSize];
            rowIndexBuffer.get(rowIndexBytes);
            
            List<SSTable.RowIndexEntry> rowIndex = objectMapper.readValue(
                rowIndexBytes,
                objectMapper.getTypeFactory().constructCollectionType(List.class, SSTable.RowIndexEntry.class)
            );
            
            // 更新blockInfo的minKey和maxKey
            if (blockInfo.minKey == null || blockInfo.maxKey == null) {
                if (!rowIndex.isEmpty()) {
                    blockInfo.updateKeys(rowIndex.get(0).getKey(), rowIndex.get(rowIndex.size() - 1).getKey());
                }
            }
            
            // 3. 处理点查询：二分查找 + 直接读取
            if (isPointQuery && exactKey != null) {
                SSTable.RowIndexEntry targetEntry = binarySearchRowIndex(rowIndex, exactKey);
                if (targetEntry != null) {
                    Row row = dataBlock.getRowByIndex(targetEntry);
                    if (row != null) {
                        rows.add(row);
                    }
                }
                return rows;
            }
            
            // 4. 处理范围查询：遍历rowIndex，按需读取
            for (SSTable.RowIndexEntry entry : rowIndex) {
                RowKey rowKey = entry.getKey();
                
                if (startKey != null && rowKey.compareTo(startKey) < 0) {
                    continue;
                }
                if (endKey != null && rowKey.compareTo(endKey) > 0) {
                    break;
                }
                
                Row row = dataBlock.getRowByIndex(entry);
                rows.add(row);
            }
            
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            logger.warn("Failed to read block at offset {}: {}", blockInfo.offset, e.getMessage());
        }
        
        return rows;
    }
    
    /**
     * 在RowIndex中二分查找指定key
     */
    private SSTable.RowIndexEntry binarySearchRowIndex(List<SSTable.RowIndexEntry> rowIndex, RowKey targetKey) {
        int left = 0;
        int right = rowIndex.size() - 1;
        
        while (left <= right) {
            int mid = left + (right - left) / 2;
            SSTable.RowIndexEntry entry = rowIndex.get(mid);
            RowKey midKey = entry.getKey();
            
            int cmp = midKey.compareTo(targetKey);
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
     * 应用过滤条件
     */
    private List<Row> applyFilter(List<Row> rows) {
        List<Row> filtered = new ArrayList<>();
        for (Row row : rows) {
            if (predicate.test(row, schema)) {
                filtered.add(row);
            }
        }
        return filtered;
    }
    
    /**
     * 应用投影
     */
    private List<Row> applyProjection(List<Row> rows) {
        if (projection.isAll()) {
            return rows;
        }
        List<Row> projected = new ArrayList<>();
        for (Row row : rows) {
            Row projectedRow = projection.project(row, schema);
            projected.add(projectedRow);
        }
        return projected;
    }
    
    private Iterator<BlockInfo> buildBlockIterator() throws IOException {
        List<BlockInfo> blocks = new ArrayList<>();
        
        long position = 0;
        long dataEndPosition = footer.getBloomFilterOffset() > 0 
                ? footer.getBloomFilterOffset() 
                : fileChannel.size() - 12;
        
        while (position < dataEndPosition) {
            try {
                fileChannel.position(position);
                
                // 读取DataBlock大小
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                int bytesRead = fileChannel.read(sizeBuffer);
                if (bytesRead < 4) {
                    break;
                }
                
                sizeBuffer.flip();
                int blockSize = sizeBuffer.getInt();
                
                if (blockSize <= 0 || blockSize > 1024 * 1024) {
                    position += 4;
                    continue;
                }
                
                if (position + 4 + blockSize > dataEndPosition) {
                    break;
                }
                
                // 跳过DataBlock，读取RowIndex大小
                long rowIndexOffsetPos = position + 4 + blockSize;
                fileChannel.position(rowIndexOffsetPos);
                
                ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
                bytesRead = fileChannel.read(rowIndexSizeBuffer);
                if (bytesRead < 4) {
                    break;
                }
                
                rowIndexSizeBuffer.flip();
                int rowIndexSize = rowIndexSizeBuffer.getInt();
                
                if (rowIndexSize <= 0 || rowIndexSize > 1024 * 1024) {
                    position += 4 + blockSize;
                    continue;
                }
                
                BlockInfo blockInfo = new BlockInfo(
                    position, 
                    blockSize, 
                    null, 
                    null,
                    rowIndexOffsetPos,
                    rowIndexSize
                );
                blocks.add(blockInfo);
                
                // 移动到下一个Block：当前position + DataBlock(4+size) + RowIndex(4+size)
                position += 4 + blockSize + 4 + rowIndexSize;
            } catch (Exception e) {
                logger.warn("Error building block iterator at position {}: {}", position, e.getMessage());
                break;
            }
        }
        
        return blocks.iterator();
    }
    
    private Iterator<BlockInfo> buildPointQueryIterator(RowKey targetKey) throws IOException {
        if (footer.getIndexOffset() <= 0) {
            return buildBlockIterator();
        }
        
        try {
            List<SSTable.BlockIndexEntry> indexEntries = loadIndexEntries();
            if (indexEntries == null || indexEntries.isEmpty()) {
                return buildBlockIterator();
            }
            
            List<BlockInfo> blocks = new ArrayList<>();
            for (SSTable.BlockIndexEntry entry : indexEntries) {
                if (entry.mightContainKey(targetKey)) {
                    blocks.add(new BlockInfo(
                        entry.getBlockOffset(),
                        entry.getBlockSize(),
                        entry.getFirstKey(),
                        entry.getLastKey(),
                        entry.getRowIndexOffset(),
                        entry.getRowIndexSize()
                    ));
                    break;
                }
            }
            
            if (!blocks.isEmpty()) {
                return blocks.iterator();
            }
        } catch (Exception e) {
            logger.warn("Failed to use index for point query: {}", e.getMessage());
        }
        
        return buildBlockIterator();
    }
    
    private List<SSTable.BlockIndexEntry> loadIndexEntries() throws IOException {
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
            
            // 先尝试读取新的BlockIndexEntry格式
            try {
                return objectMapper.readValue(
                        indexBytes, 
                        objectMapper.getTypeFactory().constructCollectionType(List.class, SSTable.BlockIndexEntry.class)
                );
            } catch (Exception e) {
                // 向后兼容：尝试读取旧的IndexEntry格式
                logger.debug("Failed to read BlockIndexEntry, trying legacy IndexEntry format");
                List<SSTable.IndexEntry> legacyEntries = objectMapper.readValue(
                        indexBytes, 
                        objectMapper.getTypeFactory().constructCollectionType(List.class, SSTable.IndexEntry.class)
                );
                // 将IndexEntry转换为BlockIndexEntry
                List<SSTable.BlockIndexEntry> result = new ArrayList<>();
                for (SSTable.IndexEntry entry : legacyEntries) {
                    result.add(entry);
                }
                return result;
            }
        } catch (Exception e) {
            logger.warn("Failed to load index entries: {}", e.getMessage());
            return null;
        }
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
    
    private static class BlockInfo {
        final long offset;          // DataBlock的偏移量
        final int size;             // DataBlock的大小
        final long rowIndexOffset;  // RowIndex的偏移量（可选）
        final int rowIndexSize;     // RowIndex的大小（可选）
        RowKey minKey;
        RowKey maxKey;
        
        BlockInfo(long offset, int size, RowKey minKey, RowKey maxKey) {
            this(offset, size, minKey, maxKey, 0, 0);
        }
        
        BlockInfo(long offset, int size, RowKey minKey, RowKey maxKey,
                 long rowIndexOffset, int rowIndexSize) {
            this.offset = offset;
            this.size = size;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.rowIndexOffset = rowIndexOffset;
            this.rowIndexSize = rowIndexSize;
        }
        
        void updateKeys(RowKey min, RowKey max) {
            this.minKey = min;
            this.maxKey = max;
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
