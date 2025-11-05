package com.minipaimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * SSTable 读取器
 * 负责从磁盘文件读取 SSTable 数据
 */
public class SSTableReader {
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);
    
    /** ObjectMapper 用于反序列化 */
    private final ObjectMapper objectMapper;
    
    public SSTableReader() {
        this.objectMapper = SerializationUtils.getObjectMapper();
    }

    /**
     * 从 SSTable 文件中读取数据
     * 
     * @param filePath 文件路径
     * @param key 要查找的键
     * @return 对应的行数据，如果不存在返回null
     * @throws IOException 读取异常
     */
    public Row get(String filePath, RowKey key) throws IOException {
        logger.debug("Reading from SSTable: {}, key: {}", filePath, key);
        
        Path path = Paths.get(filePath);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            
            // 1. 读取 Footer
            SSTable.Footer footer = readFooter(fileChannel);
            
            // 2. 使用布隆过滤器快速检查键是否存在
            if (!mightContainKey(fileChannel, footer, key)) {
                logger.debug("Key definitely not present according to bloom filter");
                return null;
            }
            
            // 3. 使用索引定位数据块
            Long dataBlockOffset = locateDataBlock(fileChannel, footer, key);
            if (dataBlockOffset == null) {
                logger.debug("Key not found in index");
                return null;
            }
            
            // 4. 在数据块中查找具体数据
            return findRowInDataBlock(fileChannel, dataBlockOffset, key);
        }
    }

    /**
     * 读取 Footer
     */
    private SSTable.Footer readFooter(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        
        // 读取魔数（最后4字节）
        ByteBuffer magicBuffer = ByteBuffer.allocate(4);
        fileChannel.read(magicBuffer, fileSize - 4);
        magicBuffer.flip();
        int magic = magicBuffer.getInt();
        
        if (magic != 0xCAFEBABE) {
            throw new IOException("Invalid SSTable file: magic number mismatch");
        }
        
        // 读取 Footer 大小（倒数第8字节开始的4字节）
        ByteBuffer footerSizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(footerSizeBuffer, fileSize - 8);
        footerSizeBuffer.flip();
        int footerSize = footerSizeBuffer.getInt();
        
        // 读取 Footer 数据
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerSize);
        fileChannel.read(footerBuffer, fileSize - 8 - 4 - footerSize);
        footerBuffer.flip();
        
        byte[] footerBytes = new byte[footerSize];
        footerBuffer.get(footerBytes);
        
        return objectMapper.readValue(footerBytes, SSTable.Footer.class);
    }

    /**
     * 使用布隆过滤器检查键是否可能存在
     */
    private boolean mightContainKey(FileChannel fileChannel, SSTable.Footer footer, RowKey key) 
            throws IOException {
        // 定位布隆过滤器
        fileChannel.position(footer.getBloomFilterOffset());
        
        // 读取布隆过滤器大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int bloomSize = sizeBuffer.getInt();
        
        // 读取布隆过滤器数据
        ByteBuffer bloomBuffer = ByteBuffer.allocate(bloomSize);
        fileChannel.read(bloomBuffer);
        bloomBuffer.flip();
        
        byte[] bloomBytes = new byte[bloomSize];
        bloomBuffer.get(bloomBytes);
        
        // 反序列化布隆过滤器
        ByteArrayInputStream bais = new ByteArrayInputStream(bloomBytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(ois, Funnels.byteArrayFunnel());
            return bloomFilter.mightContain(key.getBytes());
        }
    }

    /**
     * 使用索引定位数据块
     */
    private Long locateDataBlock(FileChannel fileChannel, SSTable.Footer footer, RowKey key) 
            throws IOException {
        // 定位索引块
        fileChannel.position(footer.getIndexOffset());
        
        // 读取索引块大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int indexSize = sizeBuffer.getInt();
        
        // 读取索引块数据
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize);
        fileChannel.read(indexBuffer);
        indexBuffer.flip();
        
        byte[] indexBytes = new byte[indexSize];
        indexBuffer.get(indexBytes);
        
        // 反序列化索引条目
        List<SSTable.IndexEntry> indexEntries = objectMapper.readValue(
                indexBytes, 
                objectMapper.getTypeFactory().constructCollectionType(List.class, SSTable.IndexEntry.class)
        );
        
        // 简化实现：找到第一个大于等于目标键的索引条目
        for (SSTable.IndexEntry entry : indexEntries) {
            if (entry.getKey().compareTo(key) >= 0) {
                return entry.getOffset();
            }
        }
        
        // 如果没有找到，返回最后一个数据块
        if (!indexEntries.isEmpty()) {
            return indexEntries.get(indexEntries.size() - 1).getOffset();
        }
        
        return null;
    }

    /**
     * 在数据块中查找具体数据
     */
    private Row findRowInDataBlock(FileChannel fileChannel, long blockOffset, RowKey key) 
            throws IOException {
        // 定位数据块
        fileChannel.position(blockOffset);
        
        // 读取数据块大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(sizeBuffer);
        sizeBuffer.flip();
        int blockSize = sizeBuffer.getInt();
        
        // 读取数据块数据
        ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
        fileChannel.read(blockBuffer);
        blockBuffer.flip();
        
        byte[] blockBytes = new byte[blockSize];
        blockBuffer.get(blockBytes);
        
        // 反序列化数据块
        SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
        
        // 在数据块中查找目标行
        for (SSTable.RowData rowData : dataBlock.getRows()) {
            if (rowData.getKey().equals(key)) {
                return rowData.getRow();
            }
        }
        
        return null;
    }

    /**
     * 扫描指定范围的数据
     * 
     * @param filePath 文件路径
     * @param startKey 起始键（包含）
     * @param endKey 结束键（包含）
     * @return 范围内的所有行数据
     * @throws IOException 读取异常
     */
    public void scan(String filePath, RowKey startKey, RowKey endKey, ScanCallback callback) 
            throws IOException {
        logger.debug("Scanning SSTable: {} from {} to {}", filePath, startKey, endKey);
        
        Path path = Paths.get(filePath);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            
            // 1. 读取 Footer
            SSTable.Footer footer = readFooter(fileChannel);
            
            // 2. 遍历所有数据块
            // 简化实现：读取所有数据块并过滤
            scanAllDataBlocks(fileChannel, startKey, endKey, callback);
        }
    }

    /**
     * 扫描所有数据块
     */
    private void scanAllDataBlocks(FileChannel fileChannel, RowKey startKey, RowKey endKey, 
                                  ScanCallback callback) throws IOException {
        // 简化实现：从文件开始位置顺序读取所有数据块
        long position = 0;
        long fileSize = fileChannel.size() - 8 - 4 - readFooterSize(fileChannel); // 减去footer和magic
        
        while (position < fileSize) {
            fileChannel.position(position);
            
            // 读取数据块大小
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            int bytesRead = fileChannel.read(sizeBuffer);
            if (bytesRead < 4) {
                break; // 文件结束
            }
            
            sizeBuffer.flip();
            int blockSize = sizeBuffer.getInt();
            
            if (blockSize <= 0 || blockSize > 1024 * 1024) { // 防止异常大小
                position += 4;
                continue;
            }
            
            // 读取数据块数据
            ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
            fileChannel.read(blockBuffer);
            blockBuffer.flip();
            
            byte[] blockBytes = new byte[blockSize];
            blockBuffer.get(blockBytes);
            
            try {
                // 反序列化数据块
                SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
                
                // 处理数据块中的每一行
                for (SSTable.RowData rowData : dataBlock.getRows()) {
                    RowKey rowKey = rowData.getKey();
                    // 检查是否在范围内
                    if (rowKey.compareTo(startKey) >= 0 && rowKey.compareTo(endKey) <= 0) {
                        callback.onRow(rowKey, rowData.getRow());
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to read data block at position {}: {}", position, e.getMessage());
            }
            
            position += 4 + blockSize;
        }
    }

    /**
     * 读取 Footer 大小
     */
    private int readFooterSize(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        ByteBuffer footerSizeBuffer = ByteBuffer.allocate(4);
        fileChannel.read(footerSizeBuffer, fileSize - 8);
        footerSizeBuffer.flip();
        return footerSizeBuffer.getInt();
    }

    /**
     * 扫描回调接口
     */
    public interface ScanCallback {
        void onRow(RowKey key, Row row);
    }
}
