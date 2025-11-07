package com.mini.paimon.storage;

import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * LSM Tree Compaction
 * 负责多层SSTable的合并压缩
 */
public class Compactor {
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);
    
    private static final int MAX_LEVEL = 7;
    private static final int LEVEL0_COMPACTION_TRIGGER = 4;
    private static final int SIZE_RATIO = 10;
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final SSTableReader reader;
    private final SSTableWriter writer;
    private final AtomicLong sequenceGenerator;
    
    public Compactor(Schema schema, PathFactory pathFactory, String database, String table,
                    AtomicLong sequenceGenerator) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.reader = new SSTableReader();
        this.writer = new SSTableWriter();
        this.sequenceGenerator = sequenceGenerator;
    }
    
    /**
     * 检查是否需要Compaction
     */
    public boolean needsCompaction(List<LeveledSSTable> sstables) {
        Map<Integer, List<LeveledSSTable>> levelMap = groupByLevel(sstables);
        
        // Level 0: 文件数超过阈值
        if (levelMap.getOrDefault(0, Collections.emptyList()).size() >= LEVEL0_COMPACTION_TRIGGER) {
            return true;
        }
        
        // 其他层级: 检查大小比例
        for (int level = 1; level < MAX_LEVEL; level++) {
            List<LeveledSSTable> currentLevel = levelMap.getOrDefault(level, Collections.emptyList());
            List<LeveledSSTable> nextLevel = levelMap.getOrDefault(level + 1, Collections.emptyList());
            
            long currentSize = currentLevel.stream().mapToLong(LeveledSSTable::getSize).sum();
            long nextSize = nextLevel.stream().mapToLong(LeveledSSTable::getSize).sum();
            
            if (nextSize > 0 && currentSize / nextSize > SIZE_RATIO) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 执行Compaction
     */
    public CompactionResult compact(List<LeveledSSTable> sstables) throws IOException {
        Map<Integer, List<LeveledSSTable>> levelMap = groupByLevel(sstables);
        
        // 选择需要压缩的层级
        int compactionLevel = selectCompactionLevel(levelMap);
        if (compactionLevel < 0) {
            return CompactionResult.empty();
        }
        
        logger.info("Starting compaction at level {}", compactionLevel);
        
        List<LeveledSSTable> inputFiles = selectCompactionFiles(levelMap, compactionLevel);
        if (inputFiles.isEmpty()) {
            return CompactionResult.empty();
        }
        
        // 执行合并
        List<LeveledSSTable> outputFiles = mergeFiles(inputFiles, compactionLevel + 1);
        
        logger.info("Compaction completed: {} input files -> {} output files", 
                   inputFiles.size(), outputFiles.size());
        
        return new CompactionResult(inputFiles, outputFiles);
    }
    
    /**
     * 选择需要压缩的层级
     */
    private int selectCompactionLevel(Map<Integer, List<LeveledSSTable>> levelMap) {
        // Level 0 优先
        if (levelMap.getOrDefault(0, Collections.emptyList()).size() >= LEVEL0_COMPACTION_TRIGGER) {
            return 0;
        }
        
        // 其他层级按大小比例
        for (int level = 1; level < MAX_LEVEL; level++) {
            List<LeveledSSTable> currentLevel = levelMap.getOrDefault(level, Collections.emptyList());
            List<LeveledSSTable> nextLevel = levelMap.getOrDefault(level + 1, Collections.emptyList());
            
            long currentSize = currentLevel.stream().mapToLong(LeveledSSTable::getSize).sum();
            long nextSize = nextLevel.stream().mapToLong(LeveledSSTable::getSize).sum();
            
            if (nextSize > 0 && currentSize / nextSize > SIZE_RATIO) {
                return level;
            }
        }
        
        return -1;
    }
    
    /**
     * 选择需要压缩的文件
     */
    private List<LeveledSSTable> selectCompactionFiles(Map<Integer, List<LeveledSSTable>> levelMap, 
                                                       int level) {
        List<LeveledSSTable> levelFiles = levelMap.getOrDefault(level, Collections.emptyList());
        
        if (level == 0) {
            // Level 0: 所有文件都参与压缩
            return new ArrayList<>(levelFiles);
        } else {
            // 其他层级: 选择重叠的文件
            List<LeveledSSTable> nextLevelFiles = levelMap.getOrDefault(level + 1, Collections.emptyList());
            return selectOverlappingFiles(levelFiles, nextLevelFiles);
        }
    }
    
    /**
     * 选择重叠的文件
     */
    private List<LeveledSSTable> selectOverlappingFiles(List<LeveledSSTable> currentLevel, 
                                                        List<LeveledSSTable> nextLevel) {
        List<LeveledSSTable> result = new ArrayList<>(currentLevel);
        
        // 找到与当前层级重叠的下层文件
        for (LeveledSSTable current : currentLevel) {
            for (LeveledSSTable next : nextLevel) {
                if (rangeOverlaps(current, next)) {
                    if (!result.contains(next)) {
                        result.add(next);
                    }
                }
            }
        }
        
        return result;
    }
    
    /**
     * 检查范围是否重叠
     */
    private boolean rangeOverlaps(LeveledSSTable a, LeveledSSTable b) {
        return a.getMinKey().compareTo(b.getMaxKey()) <= 0 && 
               b.getMinKey().compareTo(a.getMaxKey()) <= 0;
    }
    
    /**
     * 合并文件
     */
    private List<LeveledSSTable> mergeFiles(List<LeveledSSTable> inputFiles, int targetLevel) 
            throws IOException {
        // 读取所有输入文件的数据
        TreeMap<RowKey, Row> mergedData = new TreeMap<>();
        
        for (LeveledSSTable sst : inputFiles) {
            List<Row> rows = reader.scan(sst.getPath());
            for (Row row : rows) {
                RowKey key = schema.hasPrimaryKey() ? 
                            RowKey.fromRow(row, schema) : 
                            new RowKey(String.valueOf(System.nanoTime()).getBytes());
                mergedData.put(key, row);
            }
        }
        
        // 写入新文件
        List<LeveledSSTable> outputFiles = new ArrayList<>();
        
        MemTable memTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
        for (Map.Entry<RowKey, Row> entry : mergedData.entrySet()) {
            memTable.put(entry.getValue());
            
            // 达到大小限制时刷写
            if (memTable.getSize() >= memTable.getMaxSize()) {
                outputFiles.add(flushToLevel(memTable, targetLevel));
                memTable = new MemTable(schema, sequenceGenerator.getAndIncrement());
            }
        }
        
        // 刷写剩余数据
        if (!memTable.isEmpty()) {
            outputFiles.add(flushToLevel(memTable, targetLevel));
        }
        
        return outputFiles;
    }
    
    /**
     * 刷写到指定层级
     */
    private LeveledSSTable flushToLevel(MemTable memTable, int level) throws IOException {
        String path = pathFactory.getSSTPath(database, table, level, 
                                            memTable.getSequenceNumber()).toString();
        SSTable.Footer footer = writer.flush(memTable, path);
        
        return new LeveledSSTable(
            path,
            level,
            footer.getMinKey(),
            footer.getMaxKey(),
            Files.size(Paths.get(path)),
            footer.getRowCount()
        );
    }
    
    /**
     * 按层级分组
     */
    private Map<Integer, List<LeveledSSTable>> groupByLevel(List<LeveledSSTable> sstables) {
        return sstables.stream()
            .collect(Collectors.groupingBy(LeveledSSTable::getLevel));
    }
    
    /**
     * 带层级的SSTable
     */
    public static class LeveledSSTable {
        private final String path;
        private final int level;
        private final RowKey minKey;
        private final RowKey maxKey;
        private final long size;
        private final long rowCount;
        
        public LeveledSSTable(String path, int level, RowKey minKey, RowKey maxKey, 
                             long size, long rowCount) {
            this.path = path;
            this.level = level;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.size = size;
            this.rowCount = rowCount;
        }
        
        public String getPath() { return path; }
        public int getLevel() { return level; }
        public RowKey getMinKey() { return minKey; }
        public RowKey getMaxKey() { return maxKey; }
        public long getSize() { return size; }
        public long getRowCount() { return rowCount; }
    }
    
    /**
     * Compaction结果
     */
    public static class CompactionResult {
        private final List<LeveledSSTable> inputFiles;
        private final List<LeveledSSTable> outputFiles;
        
        public CompactionResult(List<LeveledSSTable> inputFiles, List<LeveledSSTable> outputFiles) {
            this.inputFiles = inputFiles;
            this.outputFiles = outputFiles;
        }
        
        public static CompactionResult empty() {
            return new CompactionResult(Collections.emptyList(), Collections.emptyList());
        }
        
        public List<LeveledSSTable> getInputFiles() { return inputFiles; }
        public List<LeveledSSTable> getOutputFiles() { return outputFiles; }
        public boolean isEmpty() { return inputFiles.isEmpty(); }
    }
}
