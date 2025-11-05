package com.minipaimon.storage;

import com.minipaimon.manifest.ManifestEntry;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.metadata.Schema;
import com.minipaimon.snapshot.Snapshot;
import com.minipaimon.snapshot.SnapshotManager;
import com.minipaimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LSM Tree 实现
 * 结合内存表和磁盘表，提供完整的键值存储功能
 */
public class LSMTree {
    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);
    
    /** 当前活跃的内存表 */
    private MemTable activeMemTable;
    
    /** 正在刷写的内存表 */
    private MemTable immutableMemTable;
    
    /** 表结构 */
    private final Schema schema;
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 数据库名 */
    private final String database;
    
    /** 表名 */
    private final String table;
    
    /** 序列号生成器 */
    private final AtomicLong sequenceGenerator;
    
    /** SSTable 读取器 */
    private final SSTableReader sstReader;
    
    /** SSTable 写入器 */
    private final SSTableWriter sstWriter;
    
    /** 快照管理器 */
    private final SnapshotManager snapshotManager;

    public LSMTree(Schema schema, PathFactory pathFactory, String database, String table) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.sequenceGenerator = new AtomicLong(0);
        this.sstReader = new SSTableReader();
        this.sstWriter = new SSTableWriter();
        this.snapshotManager = new SnapshotManager(pathFactory, database, table);
        
        // 初始化活跃内存表
        this.activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        
        logger.info("LSMTree initialized for table {}/{}", database, table);
    }

    /**
     * 插入数据
     * 
     * @param row 数据行
     * @throws IOException 写入异常
     */
    public synchronized void put(Row row) throws IOException {
        // 插入到活跃内存表
        activeMemTable.put(row);
        
        // 检查是否需要刷写
        if (activeMemTable.isFull()) {
            flushMemTable();
        }
    }

    /**
     * 获取数据
     * 
     * @param key 主键
     * @return 数据行，如果不存在返回null
     * @throws IOException 读取异常
     */
    public Row get(RowKey key) throws IOException {
        // 1. 先在活跃内存表中查找
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
        // 简化实现：只查找最新的 SSTable 文件
        // 实际实现中需要查找所有相关的 SSTable 文件并合并结果
        String sstPath = pathFactory.getSSTPath(database, table, 0, 0).toString();
        
        try {
            return sstReader.get(sstPath, key);
        } catch (IOException e) {
            logger.debug("SSTable not found or error reading: {}", sstPath);
            return null;
        }
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
