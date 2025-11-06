package com.minipaimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SSTable 写入器
 * 负责将内存表数据刷写到磁盘上的 SSTable 文件
 */
public class SSTableWriter {
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);
    
    /** 默认数据块大小 */
    private static final int DEFAULT_BLOCK_SIZE = 4096;
    
    /** 布隆过滤器误判率 */
    private static final double BLOOM_FILTER_FPP = 0.01;
    
    /** JSON 序列化工具 */
    private final ObjectMapper objectMapper;
    
    public SSTableWriter() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 将 MemTable 刷写到 SSTable 文件
     * 
     * @param memTable 内存表
     * @param filePath 文件路径
     * @return SSTable 元信息
     * @throws IOException 写入异常
     */
    public SSTable.Footer flush(MemTable memTable, String filePath) throws IOException {
        logger.info("Flushing MemTable to SSTable: {}", filePath);
        
        // 获取内存表中的所有条目
        Map<RowKey, Row> entries = memTable.getEntries();
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("MemTable is empty");
        }
        
        // 创建文件
        Path path = Paths.get(filePath);
        File file = path.toFile();
        file.getParentFile().mkdirs();
        
        try (FileChannel fileChannel = FileChannel.open(path, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            // 1. 写入数据块
            List<SSTable.DataBlock> dataBlocks = writeDataBlocks(fileChannel, entries);
            
            // 2. 写入布隆过滤器
            long bloomFilterOffset = fileChannel.position();
            BloomFilter<byte[]> bloomFilter = writeBloomFilter(fileChannel, entries.keySet());
            long bloomFilterSize = fileChannel.position() - bloomFilterOffset;
            
            // 3. 写入索引块
            long indexOffset = fileChannel.position();
            List<SSTable.IndexEntry> indexEntries = writeIndexBlock(fileChannel, dataBlocks);
            long indexSize = fileChannel.position() - indexOffset;
            
            // 4. 构建 Footer
            SSTable.Footer footer = buildFooter(
                    indexOffset, indexSize,
                    bloomFilterOffset, bloomFilterSize,
                    dataBlocks.size(), entries.size(),
                    indexEntries
            );
            
            // 5. 写入 Footer
            writeFooter(fileChannel, footer);
            
            // 强制刷新到磁盘
            fileChannel.force(true);
            
            logger.info("Successfully flushed MemTable to SSTable with {} entries, {} data blocks", 
                       entries.size(), dataBlocks.size());
            
            return footer;
        }
    }

    /**
     * 写入数据块
     */
    private List<SSTable.DataBlock> writeDataBlocks(FileChannel fileChannel, Map<RowKey, Row> entries) 
            throws IOException {
        List<SSTable.DataBlock> dataBlocks = new ArrayList<>();
        List<SSTable.RowData> currentBlockRows = new ArrayList<>();
        int currentBlockSize = 0;
        
        // 遍历所有条目
        for (Map.Entry<RowKey, Row> entry : entries.entrySet()) {
            SSTable.RowData rowData = new SSTable.RowData(entry.getKey(), entry.getValue());
            int rowSize = estimateRowSize(rowData);
            
            // 检查是否需要创建新的数据块
            if (!currentBlockRows.isEmpty() && currentBlockSize + rowSize > DEFAULT_BLOCK_SIZE) {
                // 写入当前数据块
                SSTable.DataBlock dataBlock = new SSTable.DataBlock(new ArrayList<>(currentBlockRows));
                writeDataBlock(fileChannel, dataBlock);
                dataBlocks.add(dataBlock);
                
                // 重置当前块
                currentBlockRows.clear();
                currentBlockSize = 0;
            }
            
            currentBlockRows.add(rowData);
            currentBlockSize += rowSize;
        }
        
        // 写入最后一个数据块（如果有数据）
        if (!currentBlockRows.isEmpty()) {
            SSTable.DataBlock dataBlock = new SSTable.DataBlock(new ArrayList<>(currentBlockRows));
            writeDataBlock(fileChannel, dataBlock);
            dataBlocks.add(dataBlock);
        }
        
        return dataBlocks;
    }

    /**
     * 写入单个数据块
     */
    private void writeDataBlock(FileChannel fileChannel, SSTable.DataBlock dataBlock) throws IOException {
        // 序列化数据块
        byte[] blockData = objectMapper.writeValueAsBytes(dataBlock);
        
        // 写入数据块大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(blockData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        // 写入数据块数据
        ByteBuffer dataBuffer = ByteBuffer.wrap(blockData);
        fileChannel.write(dataBuffer);
    }

    /**
     * 估算行大小
     */
    private int estimateRowSize(SSTable.RowData rowData) {
        try {
            return rowData.getKey().size() + objectMapper.writeValueAsBytes(rowData.getRow()).length + 100; // 预留一些空间
        } catch (Exception e) {
            return 1024; // 默认大小
        }
    }

    /**
     * 写入布隆过滤器
     */
    private BloomFilter<byte[]> writeBloomFilter(FileChannel fileChannel, java.util.Set<RowKey> keys) 
            throws IOException {
        // 创建布隆过滤器
        BloomFilter<byte[]> bloomFilter = BloomFilter.create(
                Funnels.byteArrayFunnel(), 
                keys.size(), 
                BLOOM_FILTER_FPP
        );
        
        // 添加所有键到布隆过滤器
        for (RowKey key : keys) {
            bloomFilter.put(key.getBytes());
        }
        
        // 序列化布隆过滤器
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            bloomFilter.writeTo(oos);
        }
        
        byte[] bloomData = baos.toByteArray();
        
        // 写入布隆过滤器大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(bloomData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        // 写入布隆过滤器数据
        ByteBuffer dataBuffer = ByteBuffer.wrap(bloomData);
        fileChannel.write(dataBuffer);
        
        return bloomFilter;
    }

    /**
     * 写入索引块
     */
    private List<SSTable.IndexEntry> writeIndexBlock(FileChannel fileChannel, List<SSTable.DataBlock> dataBlocks) 
            throws IOException {
        List<SSTable.IndexEntry> indexEntries = new ArrayList<>();
        long currentOffset = 0;
        
        // 为每个数据块创建索引条目（简化实现，使用第一个键作为索引）
        for (SSTable.DataBlock dataBlock : dataBlocks) {
            if (!dataBlock.getRows().isEmpty()) {
                RowKey firstKey = dataBlock.getRows().get(0).getKey();
                SSTable.IndexEntry indexEntry = new SSTable.IndexEntry(firstKey, currentOffset);
                indexEntries.add(indexEntry);
                
                // 更新偏移量（简化计算）
                currentOffset += 4096; // 假设每个块大约4KB
            }
        }
        
        // 序列化索引条目
        byte[] indexData = objectMapper.writeValueAsBytes(indexEntries);
        
        // 写入索引块大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(indexData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        // 写入索引块数据
        ByteBuffer dataBuffer = ByteBuffer.wrap(indexData);
        fileChannel.write(dataBuffer);
        
        return indexEntries;
    }

    /**
     * 构建 Footer
     */
    private SSTable.Footer buildFooter(long indexOffset, long indexSize,
                                      long bloomFilterOffset, long bloomFilterSize,
                                      int dataBlockCount, long rowCount,
                                      List<SSTable.IndexEntry> indexEntries) {
        // 获取最小和最大键
        RowKey minKey = indexEntries.get(0).getKey();
        RowKey maxKey = indexEntries.get(indexEntries.size() - 1).getKey();
        
        return new SSTable.Footer(
                indexOffset, indexSize,
                bloomFilterOffset, bloomFilterSize,
                dataBlockCount, rowCount,
                minKey, maxKey
        );
    }

    /**
     * 写入 Footer
     */
    private void writeFooter(FileChannel fileChannel, SSTable.Footer footer) throws IOException {
        // 序列化 Footer
        byte[] footerData = objectMapper.writeValueAsBytes(footer);
        
        // 写入 Footer 数据
        ByteBuffer dataBuffer = ByteBuffer.wrap(footerData);
        fileChannel.write(dataBuffer);
        
        // 写入 Footer 大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(footerData.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        // 写入魔数（标识文件结尾）
        ByteBuffer magicBuffer = ByteBuffer.allocate(4);
        magicBuffer.putInt(0xCAFEBABE);
        magicBuffer.flip();
        fileChannel.write(magicBuffer);
    }
}