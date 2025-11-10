package com.mini.paimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LSM Tree 存储引擎
 * 参考 Paimon 设计，负责数据的读写操作
 */
public class LSMTree {
    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final AtomicLong sequenceGenerator;
    private final boolean autoSnapshot;  // 是否自动创建快照
    
    private MemTable activeMemTable;
    private MemTable immutableMemTable;
    
    private final SSTableReader sstReader;
    private final SSTableWriter sstWriter;
    private final SnapshotManager snapshotManager;
    private final ObjectMapper objectMapper;
    private WriteAheadLog wal;
    private final Compactor compactor;
    private final AsyncCompactor asyncCompactor;  // 异步 Compaction 执行器
    private final List<Compactor.LeveledSSTable> sstables;
    private final AtomicLong walSequence;
    
    // 跟踪本次写入会话中新生成的 SSTable 文件元信息
    private final List<com.mini.paimon.manifest.DataFileMeta> pendingFiles = new ArrayList<>();
    
    public LSMTree(Schema schema, PathFactory pathFactory, String database, String table) throws IOException {
        this(schema, pathFactory, database, table, true, true);
    }
    
    public LSMTree(Schema schema, PathFactory pathFactory, String database, String table, boolean autoSnapshot) throws IOException {
        this(schema, pathFactory, database, table, autoSnapshot, true);
    }
    
    /**
     * LSMTree 构造函数
     * @param autoSnapshot 是否自动创建快照
     * @param loadExistingFiles 是否加载现有的 SSTable 文件（分区表的 Bucket 应该设为 false）
     */
    public LSMTree(Schema schema, PathFactory pathFactory, String database, String table, 
                   boolean autoSnapshot, boolean loadExistingFiles) throws IOException {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        // 关键修复：使用当前时间戳作为初始 sequence，避免不同 LSMTree 实例产生相同的文件名
        this.sequenceGenerator = new AtomicLong(System.currentTimeMillis());
        this.walSequence = new AtomicLong(0);
        this.sstReader = new SSTableReader();
        this.sstWriter = new SSTableWriter();
        this.snapshotManager = new SnapshotManager(pathFactory, database, table);
        this.objectMapper = new ObjectMapper();
        this.sstables = new ArrayList<>();
        this.compactor = new Compactor(schema, pathFactory, database, table, sequenceGenerator);
        this.asyncCompactor = new AsyncCompactor(schema, pathFactory, database, table, sequenceGenerator);
        this.autoSnapshot = autoSnapshot;
        
        pathFactory.createTableDirectories(database, table);
        
        this.activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        
        int recoveredCount = recoverFromWAL();
        
        // 关键修复：分区表的 Bucket LSMTree 不加载现有文件
        // 因为分区表的数据文件由 Snapshot 管理，而不是直接由 LSMTree 加载
        if (loadExistingFiles) {
            loadExistingSSTables();
        }
        
        Path walPath = pathFactory.getWalPath(database, table, walSequence.get());
        this.wal = new WriteAheadLog(walPath);
        
        logger.info("LSMTree initialized for table {}/{}, recovered {} rows from WAL, loaded {} SSTables, autoSnapshot={}", 
                   database, table, recoveredCount, sstables.size(), autoSnapshot);
    }
    
    /**
     * 向 LSM Tree 中插入或更新数据
     * 
     * @param row 数据行
     * @throws IOException 写入异常
     */
    public synchronized void put(Row row) throws IOException {
        // 1. 先写WAL
        wal.append(row);
        
        // 2. 写入活跃内存表
        int rowSize = activeMemTable.put(row);
        
        logger.debug("Put row, size: {} bytes", rowSize);
        
        // 3. 检查是否需要刷写
        if (activeMemTable.getSize() >= activeMemTable.getMaxSize() * 0.8) {
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
        
        // 3. 在磁盘上的 SSTable 中查找（按层级从低到高）
        List<Compactor.LeveledSSTable> sortedSSTables = new ArrayList<>(sstables);
        sortedSSTables.sort((a, b) -> Integer.compare(a.getLevel(), b.getLevel()));
        
        for (Compactor.LeveledSSTable sst : sortedSSTables) {
            row = sstReader.get(sst.getPath(), key);
            if (row != null) {
                return row;
            }
        }
        
        return null;
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
        
        // 3. 从所有SSTable中获取数据
        for (Compactor.LeveledSSTable sst : sstables) {
            try {
                List<Row> sstRows = sstReader.scan(sst.getPath());
                allRows.addAll(sstRows);
            } catch (Exception e) {
                logger.warn("Error reading SSTable {}: {}", sst.getPath(), e.getMessage());
            }
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
        
        // 创建新的活跃内存表和WAL
        activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        
        // 切换WAL
        wal.close();
        walSequence.incrementAndGet();
        Path newWalPath = pathFactory.getWalPath(database, table, walSequence.get());
        wal = new WriteAheadLog(newWalPath);
        
        // 异步刷写不可变内存表到磁盘
        flushImmutableMemTable();
        
        // 检查是否需要 Compaction（异步执行，不阻塞写入）
        if (asyncCompactor.needsCompaction(sstables)) {
            logger.info("Compaction needed, submitting async task");
            performAsyncCompaction();
        }
    }

    /**
     * 恢复 WAL 数据
     * 只恢复未持久化到 SSTable 的数据
     */
    private int recoverFromWAL() throws IOException {
        int totalRecovered = 0;
        
        // 扫描所有 WAL 文件
        Path walDir = pathFactory.getWalDir(database, table);
        if (!Files.exists(walDir)) {
            return 0;
        }
        
        // 查找所有 WAL 文件并按序号排序
        List<Path> walFiles = new ArrayList<>();
        try {
            Files.list(walDir)
                .filter(path -> path.getFileName().toString().endsWith(".log"))
                .forEach(walFiles::add);
        } catch (IOException e) {
            logger.warn("Failed to list WAL files: {}", e.getMessage());
            return 0;
        }
        
        // 恢复每个 WAL 文件
        for (Path walFile : walFiles) {
            try {
                WriteAheadLog tempWal = new WriteAheadLog(walFile);
                List<Row> rows = tempWal.recover();
                tempWal.close();
                
                // 将数据插入到活跃内存表
                for (Row row : rows) {
                    activeMemTable.put(row);
                }
                
                totalRecovered += rows.size();
                logger.info("Recovered {} rows from WAL: {}", rows.size(), walFile);
                
                // 恢复后删除 WAL 文件
                Files.deleteIfExists(walFile);
                logger.debug("Deleted recovered WAL: {}", walFile);
                
            } catch (Exception e) {
                logger.warn("Failed to recover WAL {}: {}", walFile, e.getMessage());
            }
        }
        
        return totalRecovered;
    }
    
    /**
     * 加载已存在的 SSTable 文件
     * 从 data 目录扫描所有 .sst 文件并加载元信息
     */
    private void loadExistingSSTables() throws IOException {
        Path dataDir = pathFactory.getDataDir(database, table);
        if (!Files.exists(dataDir)) {
            return;
        }
        
        List<Path> sstFiles = new ArrayList<>();
        try {
            Files.list(dataDir)
                .filter(path -> path.getFileName().toString().endsWith(".sst"))
                .forEach(sstFiles::add);
        } catch (IOException e) {
            logger.warn("Failed to list SSTable files: {}", e.getMessage());
            return;
        }
        
        for (Path sstFile : sstFiles) {
            try {
                // 使用 SSTableReader.get() 的方式：先读取一个空查询获取 Footer 信息
                // 或者直接读取文件获取 footer
                java.io.RandomAccessFile raf = new java.io.RandomAccessFile(sstFile.toFile(), "r");
                long fileSize = raf.length();
                
                // 读取 Footer（最后 8 字节）
                raf.seek(fileSize - 8);
                int footerSize = raf.readInt();
                
                // 读取 Footer 数据
                raf.seek(fileSize - 8 - footerSize);
                byte[] footerBytes = new byte[footerSize];
                raf.readFully(footerBytes);
                raf.close();
                
                // 反序列化 Footer
                SSTable.Footer footer = objectMapper.readValue(footerBytes, SSTable.Footer.class);
                
                // 从文件名解析 level（格式：data-{level}-{sequence}.sst）
                String fileName = sstFile.getFileName().toString();
                int level = 0;
                try {
                    String[] parts = fileName.replace(".sst", "").split("-");
                    if (parts.length >= 2) {
                        level = Integer.parseInt(parts[1]);
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse level from filename: {}", fileName);
                }
                
                // 创建 LeveledSSTable 并添加到列表
                Compactor.LeveledSSTable sst = new Compactor.LeveledSSTable(
                    sstFile.toString(),
                    level,
                    footer.getMinKey(),
                    footer.getMaxKey(),
                    Files.size(sstFile),
                    footer.getRowCount()
                );
                sstables.add(sst);
                
                logger.debug("Loaded SSTable: {} (level={}, rows={})", 
                            fileName, level, footer.getRowCount());
                
            } catch (Exception e) {
                logger.warn("Failed to load SSTable {}: {}", sstFile, e.getMessage());
            }
        }
        
        // 按层级排序（低层级在前）
        sstables.sort((a, b) -> Integer.compare(a.getLevel(), b.getLevel()));
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
        
        // 刷写到磁盘，直接返回 DataFileMeta（包含统计信息）
        com.mini.paimon.manifest.DataFileMeta fileMeta = sstWriter.flush(
            immutableMemTable, 
            sstPath, 
            schema.getSchemaId(), 
            0  // level
        );
        
        logger.info("Flushed memtable to SSTable: {}", sstPath);
        
        // 添加到SSTable列表
        Compactor.LeveledSSTable sst = new Compactor.LeveledSSTable(
            sstPath, 0, fileMeta.getMinKey(), fileMeta.getMaxKey(),
            fileMeta.getFileSize(), fileMeta.getRowCount()
        );
        sstables.add(sst);
        
        // 关键：跟踪新生成的文件元信息，供 prepareCommit 使用
        synchronized (pendingFiles) {
            pendingFiles.add(fileMeta);
        }
        
        // 只有在自动快照模式下才创建快照
        if (autoSnapshot) {
            // 创建 Manifest 条目
            List<ManifestEntry> manifestEntries = new ArrayList<>();
            ManifestEntry entry = new ManifestEntry(
                ManifestEntry.FileKind.ADD,
                0,  // bucket
                fileMeta
            );
            manifestEntries.add(entry);
            
            // 创建快照
            snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
        }
        
        // 清理不可变内存表
        immutableMemTable = null;
        
        // 关键修复：立即删除对应的旧 WAL 文件，避免重复恢复
        // 这个 WAL 文件的数据已经持久化到 SSTable，不再需要
        if (walSequence.get() > 0) {
            Path oldWalPath = pathFactory.getWalPath(database, table, walSequence.get() - 1);
            try {
                Files.deleteIfExists(oldWalPath);
                logger.info("Deleted persisted WAL after flush: {}", oldWalPath);
            } catch (Exception e) {
                logger.warn("Failed to delete WAL: {}", oldWalPath, e);
            }
        }
    }

    /**
     * 执行Compaction
     */
    private void performCompaction() throws IOException {
        logger.info("Starting compaction");
        
        Compactor.CompactionResult result = compactor.compact(sstables);
        
        if (!result.isEmpty()) {
            // 移除输入文件
            sstables.removeAll(result.getInputFiles());
            
            // 添加输出文件
            sstables.addAll(result.getOutputFiles());
            
            // 删除旧文件
            for (Compactor.LeveledSSTable sst : result.getInputFiles()) {
                try {
                    Files.deleteIfExists(java.nio.file.Paths.get(sst.getPath()));
                    logger.debug("Deleted compacted file: {}", sst.getPath());
                } catch (Exception e) {
                    logger.warn("Failed to delete file: {}", sst.getPath(), e);
                }
            }
            
            logger.info("Compaction completed");
        }
    }
    
    /**
     * 异步执行 Compaction（不阻塞写入）
     */
    private void performAsyncCompaction() {
        // 复制当前 sstables 列表用于异步任务
        List<Compactor.LeveledSSTable> sstablesCopy = new ArrayList<>(sstables);
        
        asyncCompactor.submitCompaction(sstablesCopy, new AsyncCompactor.CompactionCallback() {
            @Override
            public void onSuccess(Compactor.CompactionResult result) {
                // 异步回调：更新 sstables 列表
                synchronized (sstables) {
                    if (!result.isEmpty()) {
                        sstables.removeAll(result.getInputFiles());
                        sstables.addAll(result.getOutputFiles());
                        
                        // 删除旧文件
                        for (Compactor.LeveledSSTable oldFile : result.getInputFiles()) {
                            try {
                                Files.deleteIfExists(java.nio.file.Paths.get(oldFile.getPath()));
                                logger.debug("Deleted old SSTable: {}", oldFile.getPath());
                            } catch (Exception e) {
                                logger.warn("Failed to delete old SSTable: {}", oldFile.getPath(), e);
                            }
                        }
                        
                        logger.info("Async compaction completed: {} input -> {} output files",
                            result.getInputFiles().size(), result.getOutputFiles().size());
                    }
                }
            }
            
            @Override
            public void onFailure(Exception e) {
                logger.error("Async compaction failed", e);
            }
        });
    }

    /**
     * 从 SSTable 中获取数据（已废弃，由get方法处理）
     */
    @Deprecated
    private Row getSSTableRow(RowKey key) throws IOException {
        for (Compactor.LeveledSSTable sst : sstables) {
            try {
                Row row = sstReader.get(sst.getPath(), key);
                if (row != null) {
                    return row;
                }
            } catch (Exception e) {
                logger.warn("Error reading SSTable {}: {}", sst.getPath(), e.getMessage());
            }
        }
        return null;
    }

    /**
     * 关闭 LSM Tree，刷写所有数据
     */
    public synchronized void close() throws IOException {
        logger.info("Closing LSMTree, flushing all data");
        
        // 关闭当前 WAL
        if (wal != null) {
            wal.close();
        }
        
        // 关闭异步 Compaction 执行器
        asyncCompactor.shutdown();
        
        // 刷写活跃内存表
        if (!activeMemTable.isEmpty()) {
            String sstPath = pathFactory.getSSTPath(database, table, 0, activeMemTable.getSequenceNumber()).toString();
            com.mini.paimon.manifest.DataFileMeta fileMeta = sstWriter.flush(
                activeMemTable, 
                sstPath, 
                schema.getSchemaId(), 
                0
            );
            
            // 添加到SSTable列表
            Compactor.LeveledSSTable sst = new Compactor.LeveledSSTable(
                sstPath, 0, fileMeta.getMinKey(), fileMeta.getMaxKey(),
                fileMeta.getFileSize(), fileMeta.getRowCount()
            );
            sstables.add(sst);
            
            // 关键：跟踪新生成的文件元信息，供 prepareCommit 使用
            synchronized (pendingFiles) {
                pendingFiles.add(fileMeta);
            }
            
            // 只在自动快照模式下创建快照
            if (autoSnapshot) {
                // 创建 Manifest 条目
                List<ManifestEntry> manifestEntries = new ArrayList<>();
                ManifestEntry entry = new ManifestEntry(
                    ManifestEntry.FileKind.ADD,
                    0,
                    fileMeta
                );
                manifestEntries.add(entry);
                
                // 创建快照
                snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
            }
            
            // 删除对应的 WAL 文件（数据已持久化）
            Path currentWalPath = pathFactory.getWalPath(database, table, walSequence.get());
            try {
                Files.deleteIfExists(currentWalPath);
                logger.info("Deleted WAL after final flush: {}", currentWalPath);
            } catch (Exception e) {
                logger.warn("Failed to delete WAL: {}", currentWalPath, e);
            }
        }
        
        // 刷写不可变内存表
        if (immutableMemTable != null && !immutableMemTable.isEmpty()) {
            String sstPath = pathFactory.getSSTPath(database, table, 0, immutableMemTable.getSequenceNumber()).toString();
            com.mini.paimon.manifest.DataFileMeta fileMeta = sstWriter.flush(
                immutableMemTable, 
                sstPath, 
                schema.getSchemaId(), 
                0
            );
            
            // 关键：跟踪新生成的文件元信息，供 prepareCommit 使用
            synchronized (pendingFiles) {
                pendingFiles.add(fileMeta);
            }
            
            // 只在自动快照模式下创建快照
            if (autoSnapshot) {
                // 创建 Manifest 条目
                List<ManifestEntry> manifestEntries = new ArrayList<>();
                ManifestEntry entry = new ManifestEntry(
                    ManifestEntry.FileKind.ADD,
                    0,
                    fileMeta
                );
                manifestEntries.add(entry);
                
                // 创建快照
                snapshotManager.createSnapshot(schema.getSchemaId(), manifestEntries);
            }
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
    
    /**
     * 获取并清空待提交的文件元信息列表
     * 用于 prepareCommit 阶段获取本次写入会话生成的所有文件
     * 
     * @return 文件元信息列表
     */
    public List<com.mini.paimon.manifest.DataFileMeta> getPendingFiles() {
        synchronized (pendingFiles) {
            List<com.mini.paimon.manifest.DataFileMeta> files = new ArrayList<>(pendingFiles);
            pendingFiles.clear();
            return files;
        }
    }
}