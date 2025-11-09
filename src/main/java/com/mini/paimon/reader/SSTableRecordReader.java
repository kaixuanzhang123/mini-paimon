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
    
    // Block 迭代器状态
    private Iterator<BlockInfo> blockIterator;
    private Iterator<Row> currentBlockRows;
    
    // 范围扫描
    private RowKey startKey;
    private RowKey endKey;
    
    public SSTableRecordReader(String filePath, Schema schema) throws IOException {
        this.filePath = filePath;
        this.schema = schema;
        this.objectMapper = new ObjectMapper();
        this.init();
    }
    
    /**
     * 设置过滤条件
     */
    public SSTableRecordReader withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    /**
     * 设置投影
     */
    public SSTableRecordReader withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    /**
     * 设置扫描范围
     */
    public SSTableRecordReader withRange(RowKey startKey, RowKey endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        return this;
    }
    
    private void init() throws IOException {
        Path path = Paths.get(filePath);
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.footer = readFooter();
        this.blockIterator = buildBlockIterator();
        this.currentBlockRows = null;
    }
    
    @Override
    public String getFilePath() {
        return filePath;
    }
    
    @Override
    public void seekToKey(RowKey key) throws IOException {
        // 重建迭代器，从指定键开始
        this.startKey = key;
        this.blockIterator = buildBlockIterator();
        this.currentBlockRows = null;
    }
    
    @Override
    public boolean hasNext() throws IOException {
        ensureCurrentBlockRows();
        return currentBlockRows != null && currentBlockRows.hasNext();
    }
    
    @Override
    public Row readRecord() throws IOException {
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
    
    /**
     * 确保当前 Block 的行迭代器已准备好
     */
    private void ensureCurrentBlockRows() throws IOException {
        while ((currentBlockRows == null || !currentBlockRows.hasNext()) && blockIterator.hasNext()) {
            BlockInfo blockInfo = blockIterator.next();
            
            // 优化：Block 级别的跳过
            if (canSkipBlock(blockInfo)) {
                logger.debug("Skipped block at offset {} due to filter", blockInfo.offset);
                continue;
            }
            
            // 读取 Block 中的行
            List<Row> rows = readBlockRows(blockInfo);
            
            // 应用过滤
            if (predicate != null) {
                rows = applyFilter(rows);
            }
            
            // 应用投影
            if (projection != null) {
                rows = applyProjection(rows);
            }
            
            currentBlockRows = rows.iterator();
        }
    }
    
    /**
     * 检查是否可以跳过此 Block
     */
    private boolean canSkipBlock(BlockInfo blockInfo) {
        // 基于 minKey/maxKey 进行 Block 级过滤
        if (blockInfo.minKey == null || blockInfo.maxKey == null) {
            return false;
        }
        
        // 范围过滤
        if (startKey != null && blockInfo.maxKey.compareTo(startKey) < 0) {
            return true;
        }
        if (endKey != null && blockInfo.minKey.compareTo(endKey) > 0) {
            return true;
        }
        
        // TODO: 未来可以基于谓词进行更精细的 Block 级过滤
        // 例如：如果谓词是 age > 30，可以检查 Block 的 maxAge 统计信息
        
        return false;
    }
    
    /**
     * 读取 Block 中的所有行
     */
    private List<Row> readBlockRows(BlockInfo blockInfo) throws IOException {
        List<Row> rows = new ArrayList<>();
        
        try {
            fileChannel.position(blockInfo.offset);
            
            // 读取 Block 大小
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuffer);
            sizeBuffer.flip();
            int blockSize = sizeBuffer.getInt();
            
            // 读取 Block 数据
            ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
            fileChannel.read(blockBuffer);
            blockBuffer.flip();
            
            byte[] blockBytes = new byte[blockSize];
            blockBuffer.get(blockBytes);
            
            // 反序列化 Block
            SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
            
            // 提取行数据，并应用范围过滤
            for (SSTable.RowData rowData : dataBlock.getRows()) {
                RowKey rowKey = rowData.getKey();
                
                // 范围过滤
                if (startKey != null && rowKey.compareTo(startKey) < 0) {
                    continue;
                }
                if (endKey != null && rowKey.compareTo(endKey) > 0) {
                    continue;
                }
                
                rows.add(rowData.getRow());
            }
            
        } catch (Exception e) {
            logger.warn("Failed to read block at offset {}: {}", blockInfo.offset, e.getMessage());
        }
        
        return rows;
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
        List<Row> projected = new ArrayList<>();
        for (Row row : rows) {
            Row projectedRow = projection.project(row, schema);
            projected.add(projectedRow);
        }
        return projected;
    }
    
    /**
     * 构建 Block 迭代器
     */
    private Iterator<BlockInfo> buildBlockIterator() throws IOException {
        List<BlockInfo> blocks = new ArrayList<>();
        
        // 从索引中构建 Block 列表
        long position = 0;
        long dataEndPosition = footer.getBloomFilterOffset() > 0 
                ? footer.getBloomFilterOffset() 
                : fileChannel.size() - 12;
        
        while (position < dataEndPosition) {
            try {
                fileChannel.position(position);
                
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
                
                // 读取 Block 的 minKey/maxKey（需要反序列化整个 Block）
                // TODO: 优化 - 在 Block Header 中存储 minKey/maxKey
                BlockInfo blockInfo = new BlockInfo(position, blockSize, null, null);
                blocks.add(blockInfo);
                
                position += 4 + blockSize;
            } catch (Exception e) {
                logger.warn("Error building block iterator at position {}: {}", position, e.getMessage());
                break;
            }
        }
        
        return blocks.iterator();
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
     * Block 信息
     */
    private static class BlockInfo {
        final long offset;
        final int size;
        final RowKey minKey;
        final RowKey maxKey;
        
        BlockInfo(long offset, int size, RowKey minKey, RowKey maxKey) {
            this.offset = offset;
            this.size = size;
            this.minKey = minKey;
            this.maxKey = maxKey;
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
