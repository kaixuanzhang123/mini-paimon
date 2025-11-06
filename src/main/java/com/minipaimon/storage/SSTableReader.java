package com.minipaimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * SSTable 读取器
 * 负责从磁盘读取 SSTable 文件中的数据
 */
public class SSTableReader {
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);
    
    /** JSON 序列化工具 */
    private final ObjectMapper objectMapper;
    
    public SSTableReader() {
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * 从 SSTable 文件中获取指定键的数据
     * 
     * @param filePath 文件路径
     * @param key 查找的键
     * @return 对应的行数据，如果未找到返回 null
     * @throws IOException 读取异常
     */
    public Row get(String filePath, RowKey key) throws IOException {
        logger.debug("Getting key {} from SSTable: {}", key, filePath);
        
        Path path = Paths.get(filePath);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            
            // 1. 读取 Footer
            SSTable.Footer footer = readFooter(fileChannel);
            
            // 2. 使用布隆过滤器快速检查键是否存在
            if (!mightContainKey(fileChannel, footer, key)) {
                logger.debug("Key {} not found in bloom filter", key);
                return null;
            }
            
            // 3. 使用索引定位数据块
            Long blockOffset = locateDataBlock(fileChannel, footer, key);
            if (blockOffset == null) {
                logger.debug("Key {} not found in index", key);
                return null;
            }
            
            // 4. 在数据块中查找具体数据
            return findRowInDataBlock(fileChannel, blockOffset, key);
        }
    }
    
    /**
     * 扫描 SSTable 文件中的所有数据
     * 
     * @param filePath 文件路径
     * @return 所有行数据的列表
     * @throws IOException 读取异常
     */
    public List<Row> scan(String filePath) throws IOException {
        logger.debug("Scanning all data from SSTable: {}", filePath);
        
        List<Row> rows = new ArrayList<>();
        
        Path path = Paths.get(filePath);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            
            // 1. 读取 Footer
            SSTable.Footer footer = readFooter(fileChannel);
            
            // 2. 遍历所有数据块
            scanAllDataBlocks(fileChannel, footer, rows);
        }
        
        return rows;
    }

    /**
     * 读取 Footer
     */
    private SSTable.Footer readFooter(FileChannel fileChannel) throws IOException {
        long fileSize = fileChannel.size();
        
        // 检查文件大小是否足够
        if (fileSize < 12) { // 至少需要12字节（4字节footer大小 + 4字节footer数据 + 4字节魔数）
            throw new IOException("Invalid SSTable file: file too small, size=" + fileSize);
        }
        
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
        
        // 检查 footer 大小是否有效
        if (footerSize <= 0 || footerSize > fileSize - 8) {
            throw new IOException("Invalid SSTable file: invalid footer size=" + footerSize);
        }
        
        // 检查文件是否有足够的空间容纳 footer
        long footerOffset = fileSize - 8 - footerSize;
        if (footerOffset < 0) {
            throw new IOException("Invalid SSTable file: not enough space for footer");
        }
        
        // 读取 Footer 数据
        ByteBuffer footerBuffer = ByteBuffer.allocate(footerSize);
        fileChannel.read(footerBuffer, footerOffset);
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
        // 检查布隆过滤器偏移量是否有效
        if (footer.getBloomFilterOffset() < 0) {
            return true; // 如果没有布隆过滤器，直接返回 true
        }
        
        try {
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
        } catch (Exception e) {
            logger.warn("Failed to read bloom filter, assuming key might exist: {}", e.getMessage());
            return true; // 出错时保守地返回 true
        }
    }

    /**
     * 使用索引定位数据块
     */
    private Long locateDataBlock(FileChannel fileChannel, SSTable.Footer footer, RowKey key) 
            throws IOException {
        // 检查索引偏移量是否有效
        if (footer.getIndexOffset() < 0) {
            return null;
        }
        
        try {
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
        } catch (Exception e) {
            logger.warn("Failed to read index, scanning all blocks: {}", e.getMessage());
        }
        
        return null;
    }

    /**
     * 在数据块中查找具体数据
     */
    private Row findRowInDataBlock(FileChannel fileChannel, long blockOffset, RowKey key) 
            throws IOException {
        try {
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
        } catch (Exception e) {
            logger.warn("Failed to read data block at offset {}: {}", blockOffset, e.getMessage());
        }
        
        return null;
    }

    /**
     * 扫描所有数据块
     */
    private void scanAllDataBlocks(FileChannel fileChannel, SSTable.Footer footer, List<Row> rows) 
            throws IOException {
        // 从第一个数据块开始扫描
        long position = 0;
        long dataEndPosition = footer.getBloomFilterOffset() > 0 ? footer.getBloomFilterOffset() : fileChannel.size() - 12; // 数据块结束位置
        
        while (position < dataEndPosition) {
            try {
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
                
                // 检查是否有足够的数据可读
                if (position + 4 + blockSize > dataEndPosition) {
                    break;
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
                    
                    // 添加数据块中的所有行到结果列表
                    for (SSTable.RowData rowData : dataBlock.getRows()) {
                        rows.add(rowData.getRow());
                    }
                } catch (Exception e) {
                    logger.warn("Failed to read data block at position {}: {}", position, e.getMessage());
                }
                
                position += 4 + blockSize;
            } catch (Exception e) {
                logger.warn("Error scanning data blocks at position {}: {}", position, e.getMessage());
                break;
            }
        }
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
            scanAllDataBlocks(fileChannel, footer, startKey, endKey, callback);
        }
    }
    
    /**
     * 扫描所有数据块并过滤范围
     */
    private void scanAllDataBlocks(FileChannel fileChannel, SSTable.Footer footer, 
                                  RowKey startKey, RowKey endKey, ScanCallback callback) 
            throws IOException {
        // 从第一个数据块开始扫描
        long position = 0;
        long dataEndPosition = footer.getBloomFilterOffset() > 0 ? footer.getBloomFilterOffset() : fileChannel.size() - 12; // 数据块结束位置
        
        while (position < dataEndPosition) {
            try {
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
                
                // 检查是否有足够的数据可读
                if (position + 4 + blockSize > dataEndPosition) {
                    break;
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
                    
                    // 添加数据块中的所有行到结果列表（过滤范围）
                    for (SSTable.RowData rowData : dataBlock.getRows()) {
                        RowKey rowKey = rowData.getKey();
                        if ((startKey == null || rowKey.compareTo(startKey) >= 0) && 
                            (endKey == null || rowKey.compareTo(endKey) <= 0)) {
                            callback.onRow(rowData.getRow());
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to read data block at position {}: {}", position, e.getMessage());
                }
                
                position += 4 + blockSize;
            } catch (Exception e) {
                logger.warn("Error scanning data blocks at position {}: {}", position, e.getMessage());
                break;
            }
        }
    }
    
    /**
     * 扫描回调接口
     */
    public interface ScanCallback {
        void onRow(Row row);
    }
}