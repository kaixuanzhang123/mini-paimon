package com.mini.paimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * SSTable 分析器
 * 用于解析和展示 SSTable 文件的内部结构
 */
public class SSTableAnalyzer {
    
    private final ObjectMapper objectMapper;
    
    public SSTableAnalyzer() {
        this.objectMapper = new ObjectMapper();
    }
    
    /**
     * 解析 SSTable 文件并以 JSON 格式展示其内部结构
     * 
     * @param filePath SSTable 文件路径
     * @return 包含文件结构信息的 JSON 字符串
     * @throws IOException 读取文件时发生错误
     */
    public String analyzeSSTable(String filePath) throws IOException {
        ObjectNode result = objectMapper.createObjectNode();
        result.put("filePath", filePath);
        
        Path path = Paths.get(filePath);
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            
            // 1. 获取文件基本信息
            long fileSize = fileChannel.size();
            result.put("fileSize", fileSize);
            
            // 2. 读取 Footer
            SSTable.Footer footer = readFooter(fileChannel);
            result.set("footer", convertFooterToJson(footer, null));
            
            // 3. 读取布隆过滤器信息
            ObjectNode bloomFilterInfo = readBloomFilterInfo(fileChannel, footer);
            result.set("bloomFilter", bloomFilterInfo);
            
            // 4. 读取索引信息
            ArrayNode indexEntries = readIndexEntries(fileChannel, footer, null);
            result.set("index", indexEntries);
            
            // 5. 读取数据块信息
            ArrayNode dataBlocks = readDataBlocks(fileChannel, footer, null);
            result.set("dataBlocks", dataBlocks);
        }
        
        // 使用 writerWithDefaultPrettyPrinter 替代 toPrettyString
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
    }
    
    /**
     * 解析 RowKey 并以可读格式展示
     * 
     * @param rowKey RowKey 对象
     * @param schema 表结构（可选，用于更精确的解析）
     * @return 可读的字符串表示
     */
    private String parseRowKey(RowKey rowKey, Schema schema) {
        if (rowKey == null) {
            return "";
        }
        
        try {
            byte[] keyBytes = rowKey.getBytes();
            if (keyBytes.length == 0) {
                return "empty";
            }
            
            // 尝试解析 keyBytes
            StringBuilder result = new StringBuilder();
            ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
            
            // 没有 schema 信息时的通用解析
            // 根据你提供的数据，看起来是整数类型，每个整数占4个字节
            boolean first = true;
            while (buffer.hasRemaining() && buffer.remaining() >= 4) {
                if (!first) {
                    result.append(",");
                }
                int intValue = buffer.getInt();
                result.append(intValue);
                first = false;
            }
            
            return result.toString();
        } catch (Exception e) {
            // 解析失败时回退到原始格式
            return rowKey.toString();
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
        fileChannel.read(footerBuffer, fileSize - 8 - footerSize);
        footerBuffer.flip();
        
        byte[] footerBytes = new byte[footerSize];
        footerBuffer.get(footerBytes);
        
        return objectMapper.readValue(footerBytes, SSTable.Footer.class);
    }
    
    /**
     * 将 Footer 转换为 JSON 格式
     */
    private ObjectNode convertFooterToJson(SSTable.Footer footer, Schema schema) {
        ObjectNode footerNode = objectMapper.createObjectNode();
        footerNode.put("indexOffset", footer.getIndexOffset());
        footerNode.put("indexSize", footer.getIndexSize());
        footerNode.put("bloomFilterOffset", footer.getBloomFilterOffset());
        footerNode.put("bloomFilterSize", footer.getBloomFilterSize());
        footerNode.put("dataBlockCount", footer.getDataBlockCount());
        footerNode.put("rowCount", footer.getRowCount());
        footerNode.put("minKey", footer.getMinKey() != null ? parseRowKey(footer.getMinKey(), schema) : "");
        footerNode.put("maxKey", footer.getMaxKey() != null ? parseRowKey(footer.getMaxKey(), schema) : "");
        return footerNode;
    }
    
    /**
     * 读取布隆过滤器信息
     */
    private ObjectNode readBloomFilterInfo(FileChannel fileChannel, SSTable.Footer footer) 
            throws IOException {
        ObjectNode bloomFilterNode = objectMapper.createObjectNode();
        
        if (footer.getBloomFilterOffset() < 0) {
            bloomFilterNode.put("exists", false);
            return bloomFilterNode;
        }
        
        bloomFilterNode.put("exists", true);
        bloomFilterNode.put("offset", footer.getBloomFilterOffset());
        bloomFilterNode.put("size", footer.getBloomFilterSize());
        
        try {
            // 定位布隆过滤器
            fileChannel.position(footer.getBloomFilterOffset());
            
            // 读取布隆过滤器大小
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            fileChannel.read(sizeBuffer);
            sizeBuffer.flip();
            int bloomSize = sizeBuffer.getInt();
            
            bloomFilterNode.put("serializedSize", bloomSize);
        } catch (Exception e) {
            bloomFilterNode.put("error", e.getMessage());
        }
        
        return bloomFilterNode;
    }
    
    /**
     * 读取索引条目
     */
    private ArrayNode readIndexEntries(FileChannel fileChannel, SSTable.Footer footer, Schema schema) 
            throws IOException {
        ArrayNode indexArray = objectMapper.createArrayNode();
        
        if (footer.getIndexOffset() < 0) {
            return indexArray;
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
            
            for (SSTable.IndexEntry entry : indexEntries) {
                ObjectNode entryNode = objectMapper.createObjectNode();
                entryNode.put("firstKey", entry.getFirstKey() != null ? parseRowKey(entry.getFirstKey(), schema) : "");
                entryNode.put("lastKey", entry.getLastKey() != null ? parseRowKey(entry.getLastKey(), schema) : "");
                entryNode.put("offset", entry.getOffset());
                entryNode.put("size", entry.getSize());
                entryNode.put("rowCount", entry.getRowCount());
                indexArray.add(entryNode);
            }
        } catch (Exception e) {
            ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.put("error", e.getMessage());
            indexArray.add(errorNode);
        }
        
        return indexArray;
    }
    
    /**
     * 读取数据块信息
     */
    private ArrayNode readDataBlocks(FileChannel fileChannel, SSTable.Footer footer, Schema schema) 
            throws IOException {
        ArrayNode dataBlocksArray = objectMapper.createArrayNode();
        
        // 从第一个数据块开始扫描
        long position = 0;
        long dataEndPosition = footer.getBloomFilterOffset() > 0 ? footer.getBloomFilterOffset() : 
                              fileChannel.size() - 12; // 数据块结束位置
        
        int blockIndex = 0;
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
                
                // 创建数据块信息节点
                ObjectNode blockNode = objectMapper.createObjectNode();
                blockNode.put("index", blockIndex++);
                blockNode.put("offset", position);
                blockNode.put("size", blockSize);
                
                // 读取数据块数据
                ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
                fileChannel.read(blockBuffer);
                blockBuffer.flip();
                
                byte[] blockBytes = new byte[blockSize];
                blockBuffer.get(blockBytes);
                
                try {
                    // 反序列化数据块
                    SSTable.DataBlock dataBlock = objectMapper.readValue(blockBytes, SSTable.DataBlock.class);
                    
                    // 读取RowIndex（紧跟在DataBlock后）
                    ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
                    fileChannel.read(rowIndexSizeBuffer);
                    rowIndexSizeBuffer.flip();
                    int rowIndexSize = rowIndexSizeBuffer.getInt();
                    
                    if (rowIndexSize > 0 && rowIndexSize < 1024 * 1024) {
                        ByteBuffer rowIndexBuffer = ByteBuffer.allocate(rowIndexSize);
                        fileChannel.read(rowIndexBuffer);
                        rowIndexBuffer.flip();
                        
                        byte[] rowIndexBytes = new byte[rowIndexSize];
                        rowIndexBuffer.get(rowIndexBytes);
                        
                        java.util.List<SSTable.RowIndexEntry> rowIndex = objectMapper.readValue(
                            rowIndexBytes,
                            objectMapper.getTypeFactory().constructCollectionType(java.util.List.class, SSTable.RowIndexEntry.class)
                        );
                        
                        // 添加行信息
                        ArrayNode rowsArray = objectMapper.createArrayNode();
                        for (SSTable.RowIndexEntry entry : rowIndex) {
                            ObjectNode rowNode = objectMapper.createObjectNode();
                            rowNode.put("key", entry.getKey() != null ? parseRowKey(entry.getKey(), schema) : "");
                            
                            // 读取实际的行数据
                            try {
                                com.mini.paimon.metadata.Row row = dataBlock.getRowByIndex(entry);
                                rowNode.put("value", row != null ? row.toString() : "");
                            } catch (Exception ex) {
                                rowNode.put("value", "Error reading row: " + ex.getMessage());
                            }
                            
                            rowsArray.add(rowNode);
                        }
                        
                        blockNode.set("rows", rowsArray);
                        blockNode.put("rowCount", rowIndex.size());
                        blockNode.put("rowIndexOffset", position + 4 + blockSize);
                        blockNode.put("rowIndexSize", rowIndexSize);
                    } else {
                        blockNode.put("error", "Invalid rowIndex size: " + rowIndexSize);
                    }
                } catch (Exception e) {
                    blockNode.put("error", "Failed to deserialize data block: " + e.getMessage());
                }
                
                dataBlocksArray.add(blockNode);
                // 移动到下一个Block：当前position + DataBlock(4+size) + RowIndex(4+size)
                position += 4 + blockSize;
                try {
                    // 跳过RowIndex
                    fileChannel.position(position);
                    ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
                    fileChannel.read(rowIndexSizeBuffer);
                    rowIndexSizeBuffer.flip();
                    int rowIndexSize = rowIndexSizeBuffer.getInt();
                    if (rowIndexSize > 0 && rowIndexSize < 1024 * 1024) {
                        position += 4 + rowIndexSize;
                    }
                } catch (Exception e) {
                    // 忽略错误，继续下一个Block
                }
            } catch (Exception e) {
                ObjectNode errorNode = objectMapper.createObjectNode();
                errorNode.put("error", "Error reading data block at position " + position + ": " + e.getMessage());
                dataBlocksArray.add(errorNode);
                break;
            }
        }
        
        return dataBlocksArray;
    }
}