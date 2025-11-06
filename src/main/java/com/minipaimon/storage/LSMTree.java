package com.minipaimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.minipaimon.exception.StorageException;
import com.minipaimon.manifest.ManifestEntry;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.metadata.Schema;
import com.minipaimon.snapshot.SnapshotManager;
import com.minipaimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LSM Tree 存储引擎主协调类
 * 负责管理内存表和磁盘文件的读写操作
 */
public class LSMTree {
    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);
    
    /** Schema 定义 */
    private final Schema schema;
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /** 序列号生成器 */
    private final AtomicLong sequenceGenerator;
    
    /** 活跃内存表 */
    private MemTable activeMemTable;
    
    /** 正在刷写的内存表 */
    private MemTable immutableMemTable;
    
    /** SSTable 读取器 */
    private final SSTableReader sstReader;
    
    /** SSTable 写入器 */
    private final SSTableWriter sstWriter;
    
    /** 快照管理器 */
    private final SnapshotManager snapshotManager;
    
    /** JSON 序列化工具 */
    private final ObjectMapper objectMapper;
    
    public LSMTree(Schema schema, PathFactory pathFactory, String database, String table) throws IOException {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.sequenceGenerator = new AtomicLong(0);
        this.sstReader = new SSTableReader();
        this.sstWriter = new SSTableWriter();
        this.snapshotManager = new SnapshotManager(pathFactory, database, table);
        this.objectMapper = new ObjectMapper();
        
        // 初始化活跃内存表
        this.activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        
        // 创建表目录结构
        pathFactory.createTableDirectories(database, table);
        
        logger.info("LSMTree initialized for table {}/{}", database, table);
    }
    
    /**
     * 向 LSM Tree 中插入或更新数据
     * 
     * @param row 数据行
     * @throws IOException 写入异常
     */
    public synchronized void put(Row row) throws IOException {
        // 写入活跃内存表
        int rowSize = activeMemTable.put(row);
        
        logger.debug("Put row, size: {} bytes", rowSize);
        
        // 检查是否需要刷写
        if (activeMemTable.getSize() >= activeMemTable.getMaxSize() * 0.8) { // 80%阈值触发刷写
            flushMemTable();
        }
    }
    
    /**
     * 从 LSM Tree 中获取数据
     * 
     * @param key 行键
     * @return 对应的行数据，如果未找到返回 null
     * @throws IOException 读取异常
     */
    public synchronized Row get(RowKey key) throws IOException {
        // 1. 在活跃内存表中查找
        Row row = activeMemTable.get(key);
        if (row != null) {
            return row;
        }
        
        // 2. 在正在刷写的内存表中查找
        if (immutableMemTable != null) {
            row = immutableMemTable.get(key);
            if (row != null) {
                return row;
            }
        }
        
        // 3. 在磁盘上的 SSTable 中查找
        return getSSTableRow(key);
    }

    /**
     * 扫描所有数据
     * 
     * @return 所有数据行的列表
     * @throws IOException 读取异常
     */
    public List<Row> scan() throws IOException {
        List<Row> allRows = new ArrayList<>();
        
        // 1. 从活跃内存表中获取数据
        Map<RowKey, Row> activeData = activeMemTable.getAllData();
        allRows.addAll(activeData.values());
        
        // 2. 从正在刷写的内存表中获取数据
        if (immutableMemTable != null) {
            Map<RowKey, Row> immutableData = immutableMemTable.getAllData();
            allRows.addAll(immutableData.values());
        }
        
        // 3. 从磁盘上的 SSTable 中获取数据
        // 查找所有存在的 SSTable 文件
        try {
            // 首先尝试读取活跃内存表刷写的文件（如果有的话）
            String sstPath1 = pathFactory.getSSTPath(database, table, 0, activeMemTable.getSequenceNumber()).toString();
            if (java.nio.file.Files.exists(java.nio.file.Paths.get(sstPath1))) {
                List<Row> sstRows1 = sstReader.scan(sstPath1);
                allRows.addAll(sstRows1);
            }
            
            // 然后尝试读取不可变内存表刷写的文件（如果有的话）
            if (immutableMemTable != null) {
                String sstPath2 = pathFactory.getSSTPath(database, table, 0, immutableMemTable.getSequenceNumber()).toString();
                if (java.nio.file.Files.exists(java.nio.file.Paths.get(sstPath2))) {
                    List<Row> sstRows2 = sstReader.scan(sstPath2);
                    allRows.addAll(sstRows2);
                }
            }
        } catch (Exception e) {
            logger.debug("Error reading SSTable files: {}", e.getMessage());
        }
        
        return allRows;
    }

    /**
     * 刷写内存表到磁盘
     */
    private void flushMemTable() throws IOException {
        logger.info("Flushing memtable to SSTable");
        
        // 设置当前活跃内存表为不可变
        immutableMemTable = activeMemTable;
        
        // 创建新的活跃内存表
        activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        
        // 异步刷写不可变内存表到磁盘
        flushImmutableMemTable();
    }

    /**
     * 刷写不可变内存表到磁盘
     */
    private void flushImmutableMemTable() throws IOException {
        if (immutableMemTable == null || immutableMemTable.isEmpty()) {
            immutableMemTable = null;
            return;
        }
        
        // 生成 SSTable 文件路径
        String sstPath = pathFactory.getSSTPath(database, table, 0, immutableMemTable.getSequenceNumber()).toString();
        
        // 刷写到磁盘
        SSTable.Footer footer = sstWriter.flush(immutableMemTable, sstPath);
        
        logger.info("Flushed memtable to SSTable: {}", sstPath);
        
        // 创建 Manifest 条目
        List<ManifestEntry> manifestEntries = new ArrayList<>();
        ManifestEntry entry = new ManifestEntry(
            ManifestEntry.FileKind.ADD,
            sstPath,
            0, // level
            footer.getMinKey(),
            footer.getMaxKey(),
            footer.getRowCount()
        );
        manifestEntries.add(entry);
        
        // 创建快照
        snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
        
        // 清理不可变内存表
        immutableMemTable = null;
    }

    /**
     * 从 SSTable 中获取数据
     */
    private Row getSSTableRow(RowKey key) throws IOException {
        // 查找所有存在的 SSTable 文件
        Row result = null;
        
        try {
            // 首先尝试读取活跃内存表刷写的文件（如果有的话）
            String sstPath1 = pathFactory.getSSTPath(database, table, 0, activeMemTable.getSequenceNumber()).toString();
            if (java.nio.file.Files.exists(java.nio.file.Paths.get(sstPath1))) {
                result = sstReader.get(sstPath1, key);
                if (result != null) {
                    return result;
                }
            }
            
            // 然后尝试读取不可变内存表刷写的文件（如果有的话）
            if (immutableMemTable != null) {
                String sstPath2 = pathFactory.getSSTPath(database, table, 0, immutableMemTable.getSequenceNumber()).toString();
                if (java.nio.file.Files.exists(java.nio.file.Paths.get(sstPath2))) {
                    result = sstReader.get(sstPath2, key);
                    if (result != null) {
                        return result;
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Error reading SSTable files: {}", e.getMessage());
        }
        
        return result;
    }

    /**
     * 关闭 LSM Tree，刷写所有数据
     */
    public synchronized void close() throws IOException {
        logger.info("Closing LSMTree, flushing all data");
        
        // 刷写活跃内存表
        if (!activeMemTable.isEmpty()) {
            String sstPath = pathFactory.getSSTPath(database, table, 0, activeMemTable.getSequenceNumber()).toString();
            SSTable.Footer footer = sstWriter.flush(activeMemTable, sstPath);
            
            // 创建 Manifest 条目
            List<ManifestEntry> manifestEntries = new ArrayList<>();
            ManifestEntry entry = new ManifestEntry(
                ManifestEntry.FileKind.ADD,
                sstPath,
                0, // level
                footer.getMinKey(),
                footer.getMaxKey(),
                footer.getRowCount()
            );
            manifestEntries.add(entry);
            
            // 创建快照
            snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
        }
        
        // 刷写不可变内存表
        if (immutableMemTable != null && !immutableMemTable.isEmpty()) {
            String sstPath = pathFactory.getSSTPath(database, table, 0, immutableMemTable.getSequenceNumber()).toString();
            SSTable.Footer footer = sstWriter.flush(immutableMemTable, sstPath);
            
            // 创建 Manifest 条目
            List<ManifestEntry> manifestEntries = new ArrayList<>();
            ManifestEntry entry = new ManifestEntry(
                ManifestEntry.FileKind.ADD,
                sstPath,
                0, // level
                footer.getMinKey(),
                footer.getMaxKey(),
                footer.getRowCount()
            );
            manifestEntries.add(entry);
            
            // 创建快照
            snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
        }
        
        logger.info("LSMTree closed successfully");
    }

    /**
     * 获取 LSM Tree 状态信息
     */
    public String getStatus() {
        return String.format("LSMTree{activeMemTable=%s, immutableMemTable=%s}", 
                           activeMemTable, immutableMemTable != null ? immutableMemTable : "null");
    }
}