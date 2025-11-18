package com.mini.paimon.storage;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * MergeTreeWriter - 主键表写入器
 * 参考 Paimon MergeTreeWriter 设计
 * 
 * 特点:
 * 1. 使用 LSM Tree 存储引擎
 * 2. 支持 UPDATE/DELETE (通过主键去重)
 * 3. 有 Merge Engine
 * 4. 需要 Compaction
 */
public class MergeTreeWriter implements RecordWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(MergeTreeWriter.class);
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String tableName;
    private final long writerId;
    private final LSMTree lsmTree;
    
    public MergeTreeWriter(
            Schema schema,
            PathFactory pathFactory,
            String database,
            String tableName,
            long writerId) throws IOException {
        this(schema, pathFactory, database, tableName, writerId, null);
    }
    
    public MergeTreeWriter(
            Schema schema,
            PathFactory pathFactory,
            String database,
            String tableName,
            long writerId,
            java.util.Map<String, List<com.mini.paimon.index.IndexType>> indexConfig) throws IOException {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.tableName = tableName;
        this.writerId = writerId;
        
        // 创建 LSM Tree (主键表使用 LSM Tree，支持索引)
        this.lsmTree = new LSMTree(schema, pathFactory, database, tableName, true, writerId, indexConfig);
        
        logger.info("Created MergeTreeWriter for table {}.{} with writerId={}, indexConfig={}", 
            database, tableName, writerId, indexConfig);
    }
    
    @Override
    public void write(Row row) throws IOException {
        // 使用 LSM Tree 写入,自动支持主键去重
        lsmTree.put(row);
    }
    
    @Override
    public List<DataFileMeta> prepareCommit() throws IOException {
        logger.info("MergeTreeWriter preparing commit for table {}.{}", database, tableName);
        
        // 关闭 LSM Tree,刷写所有数据到磁盘
        lsmTree.close();
        
        // 获取所有新生成的文件元信息
        List<DataFileMeta> files = lsmTree.getPendingFiles();
        
        logger.info("MergeTreeWriter prepared {} files for table {}.{}", 
            files.size(), database, tableName);
        
        return files;
    }
    
    @Override
    public void close() throws IOException {
        if (lsmTree != null) {
            lsmTree.close();
        }
        logger.info("Closed MergeTreeWriter for table {}.{}", database, tableName);
    }
}

