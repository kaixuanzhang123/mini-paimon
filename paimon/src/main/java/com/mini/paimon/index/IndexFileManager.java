package com.mini.paimon.index;

import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 索引文件读写器
 * 负责索引的持久化和加载
 */
public class IndexFileManager {
    
    private static final Logger logger = LoggerFactory.getLogger(IndexFileManager.class);
    
    private final PathFactory pathFactory;
    
    public IndexFileManager(PathFactory pathFactory) {
        this.pathFactory = pathFactory;
    }
    
    /**
     * 保存索引到文件
     * @param index 索引对象
     * @param database 数据库名
     * @param table 表名
     * @param dataFileName 数据文件名（用于生成索引文件名）
     * @return 索引元数据
     */
    public IndexMeta saveIndex(FileIndex index, String database, String table, String dataFileName) 
            throws IOException {
        // 生成索引文件路径
        String indexFileName = generateIndexFileName(dataFileName, index.getIndexType(), 
                                                     index.getFieldName());
        String indexFilePath = getIndexFilePath(database, table, indexFileName);
        
        // 序列化索引
        byte[] data = index.serialize();
        
        // 写入文件
        Path path = Paths.get(indexFilePath);
        Files.createDirectories(path.getParent());
        Files.write(path, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        
        logger.info("Saved index: type={}, field={}, file={}, size={} bytes", 
                   index.getIndexType(), index.getFieldName(), indexFileName, data.length);
        
        // 返回索引元数据
        return new IndexMeta(
            index.getIndexType(),
            index.getFieldName(),
            indexFileName,
            data.length,
            System.currentTimeMillis()
        );
    }
    
    /**
     * 加载索引文件
     */
    public FileIndex loadIndex(String database, String table, IndexMeta indexMeta) 
            throws IOException {
        String indexFilePath = getIndexFilePath(database, table, indexMeta.getIndexFilePath());
        Path path = Paths.get(indexFilePath);
        
        if (!Files.exists(path)) {
            throw new IOException("Index file not found: " + indexFilePath);
        }
        
        // 读取文件
        byte[] data = Files.readAllBytes(path);
        
        // 反序列化索引
        FileIndex index = IndexFactory.loadIndex(
            indexMeta.getIndexType(),
            indexMeta.getFieldName(),
            data
        );
        
        logger.debug("Loaded index: type={}, field={}, file={}", 
                    indexMeta.getIndexType(), indexMeta.getFieldName(), 
                    indexMeta.getIndexFilePath());
        
        return index;
    }
    
    /**
     * 删除索引文件
     */
    public void deleteIndex(String database, String table, String indexFileName) 
            throws IOException {
        String indexFilePath = getIndexFilePath(database, table, indexFileName);
        Path path = Paths.get(indexFilePath);
        
        if (Files.exists(path)) {
            Files.delete(path);
            logger.info("Deleted index file: {}", indexFileName);
        }
    }
    
    /**
     * 生成索引文件名
     * 格式: data-0-001_fieldName.bfi
     */
    private String generateIndexFileName(String dataFileName, IndexType indexType, String fieldName) {
        // 去掉数据文件的后缀 .sst
        String baseName = dataFileName.replace(".sst", "");
        return baseName + "_" + fieldName + indexType.getFileSuffix();
    }
    
    /**
     * 获取索引文件的绝对路径
     */
    private String getIndexFilePath(String database, String table, String indexFileName) {
        // 索引文件存储在与 snapshot 同级的 index 目录下
        return pathFactory.getTablePath(database, table) + "/index/" + indexFileName;
    }
    
    /**
     * 检查索引文件是否存在
     */
    public boolean indexExists(String database, String table, String indexFileName) {
        String indexFilePath = getIndexFilePath(database, table, indexFileName);
        return Files.exists(Paths.get(indexFilePath));
    }
}
