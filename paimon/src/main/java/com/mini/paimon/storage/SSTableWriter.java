package com.mini.paimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.mini.paimon.index.BatchIndexBuilder;
import com.mini.paimon.index.FileIndex;
import com.mini.paimon.index.IndexFileManager;
import com.mini.paimon.index.IndexMeta;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.utils.PathFactory;
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
    
    /** 索引文件管理器 */
    private final IndexFileManager indexFileManager;
    
    /** 是否启用索引 */
    private final boolean indexEnabled;
    
    /** 索引配置 */
    private final Map<String, List<com.mini.paimon.index.IndexType>> indexConfig;
    
    public SSTableWriter() {
        this(null, true, null);
    }
    
    public SSTableWriter(PathFactory pathFactory, boolean indexEnabled) {
        this(pathFactory, indexEnabled, null);
    }
    
    public SSTableWriter(PathFactory pathFactory, boolean indexEnabled, Map<String, List<com.mini.paimon.index.IndexType>> indexConfig) {
        this.objectMapper = new ObjectMapper();
        this.indexFileManager = pathFactory != null ? new IndexFileManager(pathFactory) : null;
        this.indexEnabled = indexEnabled;
        this.indexConfig = indexConfig;
    }

    /**
     * 将 MemTable 刷写到 SSTable 文件
     * 
     * @param memTable 内存表
     * @param filePath 文件路径
     * @param schemaId Schema 版本 ID
     * @param level LSM Tree 层级
     * @return 数据文件元信息（包含统计信息）
     * @throws IOException 写入异常
     */
    public com.mini.paimon.manifest.DataFileMeta flush(MemTable memTable, String filePath, int schemaId, int level) throws IOException {
        return flush(memTable, filePath, schemaId, level, null, null, null);
    }
    
    /**
     * 将 MemTable 刷写到 SSTable 文件（支持索引）
     */
    public com.mini.paimon.manifest.DataFileMeta flush(
            MemTable memTable, 
            String filePath, 
            int schemaId, 
            int level,
            Schema schema,
            String database,
            String table) throws IOException {
        logger.info("Flushing MemTable to SSTable: {}", filePath);
        
        // 获取内存表中的所有条目（已按 RowKey 排序）
        Map<RowKey, Row> entries = memTable.getEntries();
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("MemTable is empty");
        }
        
        // 创建文件
        Path path = Paths.get(filePath);
        File file = path.toFile();
        file.getParentFile().mkdirs();
        
        // 记录实际的 minKey 和 maxKey（从有序数据中直接获取）
        RowKey minKey = null;
        RowKey maxKey = null;
        
        try (FileChannel fileChannel = FileChannel.open(path, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            // 1. 写入数据块（按顺序写入，保证有序性）
            WriteResult writeResult = writeDataBlocks(fileChannel, entries);
            minKey = writeResult.minKey;
            maxKey = writeResult.maxKey;
            
            // 2. 写入布隆过滤器
            long bloomFilterOffset = fileChannel.position();
            BloomFilter<byte[]> bloomFilter = writeBloomFilter(fileChannel, entries.keySet());
            long bloomFilterSize = fileChannel.position() - bloomFilterOffset;
            
            long indexOffset = fileChannel.position();
            List<SSTable.BlockIndexEntry> indexEntries = writeIndexBlock(fileChannel, writeResult.blockMetadataList);
            long indexSize = fileChannel.position() - indexOffset;
            
            SSTable.Footer footer = new SSTable.Footer(
                    indexOffset, indexSize,
                    bloomFilterOffset, bloomFilterSize,
                    writeResult.blockMetadataList.size(), entries.size(),
                    minKey, maxKey
            );
            
            // 5. 写入 Footer
            writeFooter(fileChannel, footer);
            
            // 强制刷新到磁盘
            fileChannel.force(true);
            
            long fileSize = fileChannel.size();
            
            logger.info("Successfully flushed MemTable to SSTable: file={}, entries={}, blocks={}, minKey={}, maxKey={}", 
                       filePath, entries.size(), writeResult.blockMetadataList.size(), minKey, maxKey);
            
            // 6. 构建 DataFileMeta（包含统计信息，避免后续扫描）
            // 提取相对路径（去除绝对路径前缀）
            String fileName = Paths.get(filePath).getFileName().toString();
            
            // 7. 构建并保存索引
            List<IndexMeta> indexMetaList = new ArrayList<>();
            if (indexEnabled && schema != null && database != null && table != null && indexFileManager != null) {
                indexMetaList = buildAndSaveIndexes(entries, schema, database, table, fileName);
            }
            
            return new com.mini.paimon.manifest.DataFileMeta(
                fileName,
                fileSize,
                entries.size(),
                minKey,
                maxKey,
                schemaId,
                level,
                System.currentTimeMillis(),
                indexMetaList
            );
        }
    }

    /**
     * 写入数据块，生成Block级索引和Row级索引
     * 
     * 数据结构：
     * [DataBlock1][RowIndex1][DataBlock2][RowIndex2]...
     * 
     * 每个DataBlock后紧跟其RowIndex，这样可以在定位到Block后立即读取RowIndex
     */
    private WriteResult writeDataBlocks(FileChannel fileChannel, Map<RowKey, Row> entries) 
            throws IOException {
        List<BlockMetadata> blockMetadataList = new ArrayList<>();
        List<RowKey> currentBlockKeys = new ArrayList<>();
        List<Row> currentBlockValues = new ArrayList<>();
        int currentBlockSize = 0;
        
        RowKey minKey = null;
        RowKey maxKey = null;
        
        for (Map.Entry<RowKey, Row> entry : entries.entrySet()) {
            RowKey key = entry.getKey();
            Row value = entry.getValue();
            
            if (minKey == null) {
                minKey = key;
            }
            maxKey = key;
            
            int rowSize = estimateRowSize(key, value);
            
            // 当前Block已满，写入磁盘
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
    
    /**
     * 写入一个DataBlock及其RowIndex
     * 
     * 格式：[DataBlock][RowIndex]
     * DataBlock: [valuesData连续字节流]
     * RowIndex: [RowIndexEntry数组]，每个Entry包含(key, offset, length)
     */
    private BlockMetadata writeBlockWithRowIndex(
            FileChannel fileChannel, 
            List<RowKey> keys, 
            List<Row> values) throws IOException {
        
        long blockStartOffset = fileChannel.position();
        
        // 1. 构建valuesData连续字节流，并记录每行的offset和length
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
        
        // 2. 写入DataBlock
        SSTable.DataBlock dataBlock = new SSTable.DataBlock(valuesData, keys.size());
        byte[] blockBytes = objectMapper.writeValueAsBytes(dataBlock);
        
        // 写入Block大小
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        sizeBuffer.putInt(blockBytes.length);
        sizeBuffer.flip();
        fileChannel.write(sizeBuffer);
        
        // 写入Block数据
        ByteBuffer blockBuffer = ByteBuffer.wrap(blockBytes);
        fileChannel.write(blockBuffer);
        
        int blockSize = 4 + blockBytes.length;
        
        // 3. 写入RowIndex（紧跟在DataBlock后）
        long rowIndexOffset = fileChannel.position();
        byte[] rowIndexBytes = objectMapper.writeValueAsBytes(rowIndexEntries);
        
        // 写入RowIndex大小
        ByteBuffer rowIndexSizeBuffer = ByteBuffer.allocate(4);
        rowIndexSizeBuffer.putInt(rowIndexBytes.length);
        rowIndexSizeBuffer.flip();
        fileChannel.write(rowIndexSizeBuffer);
        
        // 写入RowIndex数据
        ByteBuffer rowIndexBuffer = ByteBuffer.wrap(rowIndexBytes);
        fileChannel.write(rowIndexBuffer);
        
        int rowIndexSize = 4 + rowIndexBytes.length;
        
        // 4. 返回Block元数据
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

    private int estimateRowSize(RowKey key, Row value) {
        try {
            return key.size() + objectMapper.writeValueAsBytes(value).length + 100;
        } catch (Exception e) {
            return 1024;
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
     * 写入Block级索引
     * 每个BlockIndexEntry包含Block的位置信息和对应RowIndex的位置信息
     */
    private List<SSTable.BlockIndexEntry> writeIndexBlock(FileChannel fileChannel, List<BlockMetadata> blockMetadataList) 
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
        
        return indexEntries;
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
    
    /**
     * 构建并保存索引
     */
    private List<IndexMeta> buildAndSaveIndexes(
            Map<RowKey, Row> entries,
            Schema schema,
            String database,
            String table,
            String dataFileName) throws IOException {
        
        List<IndexMeta> indexMetaList = new ArrayList<>();
        
        try {
            // 根据 indexConfig 创建索引构建器
            BatchIndexBuilder indexBuilder;
            if (indexConfig != null && !indexConfig.isEmpty()) {
                logger.info("Building indexes with custom config: {}", indexConfig);
                indexBuilder = new BatchIndexBuilder(schema, indexConfig);
            } else {
                // 如果没有配置，使用默认索引
                logger.debug("Building indexes with default config");
                indexBuilder = BatchIndexBuilder.createDefault(schema);
            }
            
            // 添加所有行到索引
            indexBuilder.addRows(entries);
            
            // 保存所有索引
            Map<String, List<FileIndex>> indexes = indexBuilder.getIndexes();
            for (Map.Entry<String, List<FileIndex>> entry : indexes.entrySet()) {
                for (FileIndex index : entry.getValue()) {
                    IndexMeta meta = indexFileManager.saveIndex(index, database, table, dataFileName);
                    indexMetaList.add(meta);
                }
            }
            
            logger.info("Built and saved {} indexes for file {}", indexMetaList.size(), dataFileName);
            
            // 输出索引统计信息
            if (logger.isDebugEnabled()) {
                Map<String, String> stats = indexBuilder.getStatistics();
                for (Map.Entry<String, String> stat : stats.entrySet()) {
                    logger.debug("Index stats - {}: {}", stat.getKey(), stat.getValue());
                }
            }
            
        } catch (Exception e) {
            logger.error("Failed to build indexes for file {}", dataFileName, e);
            // 索引构建失败不影响数据写入
        }
        
        return indexMetaList;
    }
}