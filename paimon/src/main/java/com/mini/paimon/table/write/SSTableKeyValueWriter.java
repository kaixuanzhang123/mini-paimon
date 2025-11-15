package com.mini.paimon.table.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.storage.SSTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;

/**
 * SSTable KeyValue Writer
 * 参考 Paimon KeyValueFileWriter 设计
 * 
 * 用于主键表的SST文件写入
 */
public class SSTableKeyValueWriter implements KeyValueFileWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(SSTableKeyValueWriter.class);
    private static final int DEFAULT_BLOCK_SIZE = 4096;
    private static final double BLOOM_FILTER_FPP = 0.01;
    
    private final Path filePath;
    private final Schema schema;
    private final ObjectMapper objectMapper;
    private final TreeMap<RowKey, Row> buffer;  // 使用TreeMap保持有序
    
    public SSTableKeyValueWriter(Path filePath, Schema schema) throws IOException {
        this.filePath = filePath;
        this.schema = schema;
        this.objectMapper = new ObjectMapper();
        this.buffer = new TreeMap<>();  // 自动排序
        
        logger.debug("Created SSTableKeyValueWriter for file: {}", filePath);
    }
    
    @Override
    public void write(Row row) throws IOException {
        // 提取key并存入有序map
        if (schema.hasPrimaryKey()) {
            RowKey rowKey = row.extractKey(schema);
            buffer.put(rowKey, row);
        } else {
            // 非主键表不应该使用SST格式
            throw new IllegalStateException("SSTable format requires primary key");
        }
    }
    
    @Override
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        // 创建父目录
        Files.createDirectories(filePath.getParent());
        
        try (FileChannel fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            // 写入数据块
            WriteResult writeResult = writeDataBlocks(fileChannel);
            
            // 写入布隆过滤器
            long bloomFilterOffset = fileChannel.position();
            writeBloomFilter(fileChannel, buffer.keySet());
            long bloomFilterSize = fileChannel.position() - bloomFilterOffset;
            
            // 写入索引块
            long indexOffset = fileChannel.position();
            writeIndexBlock(fileChannel, writeResult.blockMetadataList);
            long indexSize = fileChannel.position() - indexOffset;
            
            // 写入Footer
            SSTable.Footer footer = new SSTable.Footer(
                    indexOffset, indexSize,
                    bloomFilterOffset, bloomFilterSize,
                    writeResult.blockMetadataList.size(), buffer.size(),
                    writeResult.minKey, writeResult.maxKey
            );
            writeFooter(fileChannel, footer);
            
            fileChannel.force(true);
            
            logger.debug("Flushed {} rows to SSTable: {}", buffer.size(), filePath);
        }
    }
    
    @Override
    public long getRowCount() {
        return buffer.size();
    }
    
    @Override
    public long getFileSize() throws IOException {
        if (Files.exists(filePath)) {
            return Files.size(filePath);
        }
        return 0;
    }
    
    @Override
    public DataFileMeta getFileMeta() throws IOException {
        // 获取相对路径
        String fileName = filePath.getFileName().toString();
        String relativePath = "data/" + fileName;
        
        RowKey minKey = buffer.isEmpty() ? null : buffer.firstKey();
        RowKey maxKey = buffer.isEmpty() ? null : buffer.lastKey();
        
        return new DataFileMeta(
            relativePath,
            getFileSize(),
            getRowCount(),
            minKey,
            maxKey,
            0,  // schemaId
            0,  // level
            System.currentTimeMillis()
        );
    }
    
    @Override
    public void close() throws IOException {
        // 确保所有数据都被写入
        flush();
        buffer.clear();
        logger.debug("Closed SSTableKeyValueWriter for {}", filePath);
    }
    
    // ========== 内部写入方法 ==========
    
    private WriteResult writeDataBlocks(FileChannel fileChannel) throws IOException {
        List<BlockMetadata> blockMetadataList = new ArrayList<>();
        List<RowKey> currentBlockKeys = new ArrayList<>();
        List<Row> currentBlockValues = new ArrayList<>();
        int currentBlockSize = 0;
        
        RowKey minKey = null;
        RowKey maxKey = null;
        
        for (Map.Entry<RowKey, Row> entry : buffer.entrySet()) {
            RowKey key = entry.getKey();
            Row value = entry.getValue();
            
            if (minKey == null) {
                minKey = key;
            }
            maxKey = key;
            
            int rowSize = estimateRowSize(key, value);
            
            // Block已满，写入磁盘
            if (!currentBlockKeys.isEmpty() && currentBlockSize + rowSize > DEFAULT_BLOCK_SIZE) {
                BlockMetadata blockMeta = writeBlockWithRowIndex(
                    fileChannel, currentBlockKeys, currentBlockValues);
                blockMetadataList.add(blockMeta);
                
                currentBlockKeys.clear();
                currentBlockValues.clear();
                currentBlockSize = 0;
            }
            
            currentBlockKeys.add(key);
            currentBlockValues.add(value);
            currentBlockSize += rowSize;
        }
        
        // 写入最后一个Block
        if (!currentBlockKeys.isEmpty()) {
            BlockMetadata blockMeta = writeBlockWithRowIndex(
                fileChannel, currentBlockKeys, currentBlockValues);
            blockMetadataList.add(blockMeta);
        }
        
        return new WriteResult(blockMetadataList, minKey, maxKey);
    }
    
    private BlockMetadata writeBlockWithRowIndex(
            FileChannel fileChannel, 
            List<RowKey> keys, 
            List<Row> values) throws IOException {
        
        long blockStartOffset = fileChannel.position();
        
        // 构建valuesData和RowIndex
        List<SSTable.RowIndexEntry> rowIndexEntries = new ArrayList<>();
        ByteArrayOutputStream valuesStream = new ByteArrayOutputStream();
        
        for (int i = 0; i < keys.size(); i++) {
            RowKey key = keys.get(i);
            Row value = values.get(i);
            
            int valueOffset = valuesStream.size();
            byte[] valueBytes = objectMapper.writeValueAsBytes(value);
            valuesStream.write(valueBytes);
            int valueLength = valueBytes.length;
            
            rowIndexEntries.add(new SSTable.RowIndexEntry(key, valueOffset, valueLength));
        }
        
        byte[] valuesData = valuesStream.toByteArray();
        
        // 写入DataBlock
        SSTable.DataBlock dataBlock = new SSTable.DataBlock(valuesData, keys.size());
        byte[] blockBytes = objectMapper.writeValueAsBytes(dataBlock);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(blockBytes.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        ByteBuffer blockBuffer = ByteBuffer.wrap(blockBytes);
        fileChannel.write(blockBuffer);
        
        int blockSize = 4 + blockBytes.length;
        
        // 写入RowIndex
        long rowIndexOffset = fileChannel.position();
        byte[] rowIndexBytes = objectMapper.writeValueAsBytes(rowIndexEntries);
        
        ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
        rowIndexSizeBuffer.putInt(rowIndexBytes.length);
        rowIndexSizeBuffer.flip();
        fileChannel.write(rowIndexSizeBuffer);
        
        ByteBuffer rowIndexBuffer = ByteBuffer.wrap(rowIndexBytes);
        fileChannel.write(rowIndexBuffer);
        
        int rowIndexSize = 4 + rowIndexBytes.length;
        
        RowKey firstKey = keys.get(0);
        RowKey lastKey = keys.get(keys.size() - 1);
        
        return new BlockMetadata(
            blockStartOffset, 
            blockSize, 
            firstKey, 
            lastKey, 
            keys.size(),
            rowIndexOffset,
            rowIndexSize
        );
    }
    
    private void writeBloomFilter(FileChannel fileChannel, java.util.Set<RowKey> keys) 
            throws IOException {
        BloomFilter<byte[]> bloomFilter = BloomFilter.create(
                Funnels.byteArrayFunnel(), 
                keys.size(), 
                BLOOM_FILTER_FPP
        );
        
        for (RowKey key : keys) {
            bloomFilter.put(key.getBytes());
        }
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            bloomFilter.writeTo(oos);
        }
        
        byte[] bloomData = baos.toByteArray();
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(bloomData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        ByteBuffer dataBuffer = ByteBuffer.wrap(bloomData);
        fileChannel.write(dataBuffer);
    }
    
    private void writeIndexBlock(FileChannel fileChannel, List<BlockMetadata> blockMetadataList) 
            throws IOException {
        List<SSTable.BlockIndexEntry> indexEntries = new ArrayList<>();
        
        for (BlockMetadata blockMeta : blockMetadataList) {
            SSTable.BlockIndexEntry indexEntry = new SSTable.BlockIndexEntry(
                blockMeta.firstKey,
                blockMeta.lastKey,
                blockMeta.offset,
                blockMeta.size,
                blockMeta.rowCount,
                blockMeta.rowIndexOffset,
                blockMeta.rowIndexSize
            );
            indexEntries.add(indexEntry);
        }
        
        byte[] indexData = objectMapper.writeValueAsBytes(indexEntries);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(indexData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        ByteBuffer dataBuffer = ByteBuffer.wrap(indexData);
        fileChannel.write(dataBuffer);
    }
    
    private void writeFooter(FileChannel fileChannel, SSTable.Footer footer) throws IOException {
        byte[] footerData = objectMapper.writeValueAsBytes(footer);
        
        ByteBuffer dataBuffer = ByteBuffer.wrap(footerData);
        fileChannel.write(dataBuffer);
        
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(footerData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        ByteBuffer magicBuffer = ByteBuffer.allocate(4);
        magicBuffer.putInt(0xCAFEBABE);
        magicBuffer.flip();
        fileChannel.write(magicBuffer);
    }
    
    private int estimateRowSize(RowKey key, Row value) {
        try {
            return key.size() + objectMapper.writeValueAsBytes(value).length + 100;
        } catch (Exception e) {
            return 1024;
        }
    }
    
    // ========== 内部类 ==========
    
    private static class WriteResult {
        final List<BlockMetadata> blockMetadataList;
        final RowKey minKey;
        final RowKey maxKey;
        
        WriteResult(List<BlockMetadata> blockMetadataList, RowKey minKey, RowKey maxKey) {
            this.blockMetadataList = blockMetadataList;
            this.minKey = minKey;
            this.maxKey = maxKey;
        }
    }
    
    private static class BlockMetadata {
        final long offset;
        final int size;
        final RowKey firstKey;
        final RowKey lastKey;
        final int rowCount;
        final long rowIndexOffset;
        final int rowIndexSize;
        
        BlockMetadata(long offset, int size, RowKey firstKey, RowKey lastKey, int rowCount,
                     long rowIndexOffset, int rowIndexSize) {
            this.offset = offset;
            this.size = size;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
            this.rowCount = rowCount;
            this.rowIndexOffset = rowIndexOffset;
            this.rowIndexSize = rowIndexSize;
        }
    }
}

