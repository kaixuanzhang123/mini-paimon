package com.mini.paimon.storage;

import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.RowKey;
import com.mini.paimon.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 归并排序读取器
 * 参考 Paimon 的 MergeTreeReader 实现
 * 
 * 核心功能：
 * 1. 多路归并多个有序的 SSTable 文件
 * 2. 去重：相同 key 保留最新的数据（优先级高的文件）
 * 3. 保证输出数据的有序性
 * 4. 支持流式读取，内存占用可控
 * 
 * 应用场景：
 * - Compaction：合并多层 SSTable 文件
 * - LSM Tree scan：归并读取所有数据（MemTable + SSTables）
 * - 查询优化：有序归并多个数据源
 */
public class MergeSortedReader implements Iterator<Row>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MergeSortedReader.class);
    
    private final Schema schema;
    private final SSTableReader reader;
    private final List<String> sstablePaths;
    
    // 多路归并的优先级队列
    private final PriorityQueue<MergeElement> mergeHeap;
    
    // 每个 SSTable 的迭代器
    private final List<Iterator<Row>> iterators;
    
    // 已读取的数据缓存
    private Row nextRow;
    private RowKey lastKey;
    
    // 无主键表的序列号生成器（仅用于排序，不影响实际数据）
    private long sequenceCounter = 0;
    
    // 标记数据源类型
    private final boolean isFileBasedReader;
    
    /**
     * 私有构造函数
     */
    private MergeSortedReader(Schema schema, List<String> sstablePaths, boolean isFileBased) throws IOException {
        this.schema = schema;
        this.isFileBasedReader = isFileBased;
        this.iterators = new ArrayList<>();
        this.mergeHeap = new PriorityQueue<>((a, b) -> {
            int keyCompare = a.key.compareTo(b.key);
            if (keyCompare != 0) {
                return keyCompare;
            }
            // 相同 key，优先级高的文件（readerIndex 大）排在前面
            return Integer.compare(b.readerIndex, a.readerIndex);
        });
        
        if (isFileBased) {
            // 基于文件的读取
            this.reader = new SSTableReader();
            this.sstablePaths = new ArrayList<>(sstablePaths);
            
            // 初始化所有 SSTable 的迭代器
            for (String path : sstablePaths) {
                try {
                    List<Row> rows = reader.scan(path);
                    iterators.add(rows.iterator());
                } catch (IOException e) {
                    logger.warn("Failed to open SSTable {}: {}", path, e.getMessage());
                    iterators.add(Collections.emptyIterator());
                }
            }
        } else {
            // 基于内存数据源的读取
            this.reader = null;
            this.sstablePaths = Collections.emptyList();
        }
        
        // 初始化堆：从每个迭代器读取第一个元素
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<Row> iter = iterators.get(i);
            if (iter.hasNext()) {
                Row row = iter.next();
                RowKey key = extractKey(row, i);
                mergeHeap.offer(new MergeElement(key, row, i));
            }
        }
        
        // 预读取下一行
        advance();
        
        logger.debug("Initialized MergeSortedReader with {} sources", iterators.size());
    }
    
    /**
     * 向后兼容的构造函数（用于 Compactor）
     */
    public MergeSortedReader(Schema schema, List<String> sstablePaths) throws IOException {
        this(schema, sstablePaths, true);
    }
    
    /**
     * 创建基于 SSTable 文件的归并读取器
     * 
     * @param schema 表结构
     * @param sstablePaths SSTable 文件路径列表（按优先级排序，后面的文件优先级高）
     */
    public static MergeSortedReader fromFiles(Schema schema, List<String> sstablePaths) throws IOException {
        return new MergeSortedReader(schema, sstablePaths, true);
    }
    
    /**
     * 创建基于内存数据源的归并读取器
     * 用于 LSM Tree scan，支持 MemTable + SSTables 的归并
     * 
     * @param schema 表结构
     * @param dataSources 数据源迭代器列表（按优先级排序，后面的优先级高）
     */
    public static MergeSortedReader fromIterators(Schema schema, List<Iterator<Row>> dataSources) throws IOException {
        MergeSortedReader reader = new MergeSortedReader(schema, Collections.emptyList(), false);
        reader.iterators.addAll(dataSources);
        
        // 初始化堆：从每个数据源读取第一个元素
        for (int i = 0; i < reader.iterators.size(); i++) {
            Iterator<Row> iter = reader.iterators.get(i);
            if (iter.hasNext()) {
                Row row = iter.next();
                RowKey key = reader.extractKey(row, i);
                reader.mergeHeap.offer(new MergeElement(key, row, i));
            }
        }
        
        // 预读取下一行
        reader.advance();
        
        logger.debug("Initialized MergeSortedReader from iterators with {} sources", dataSources.size());
        return reader;
    }
    
    @Override
    public boolean hasNext() {
        return nextRow != null;
    }
    
    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows");
        }
        
        Row current = nextRow;
        try {
            advance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to advance reader", e);
        }
        return current;
    }
    
    private void advance() throws IOException {
        nextRow = null;
        
        while (!mergeHeap.isEmpty() && nextRow == null) {
            MergeElement current = mergeHeap.poll();
            
            if (nextRow == null) {
                nextRow = current.row;
                lastKey = current.key;
                
                int readerIndex = current.readerIndex;
                Iterator<Row> iter = iterators.get(readerIndex);
                if (iter.hasNext()) {
                    Row nextRowFromSameReader = iter.next();
                    RowKey nextKey = extractKey(nextRowFromSameReader, readerIndex);
                    mergeHeap.offer(new MergeElement(nextKey, nextRowFromSameReader, readerIndex));
                }
                
                while (!mergeHeap.isEmpty() && mergeHeap.peek().key.equals(lastKey)) {
                    MergeElement duplicate = mergeHeap.poll();
                    int dupReaderIndex = duplicate.readerIndex;
                    Iterator<Row> dupIter = iterators.get(dupReaderIndex);
                    if (dupIter.hasNext()) {
                        Row nextRowFromDupReader = dupIter.next();
                        RowKey nextDupKey = extractKey(nextRowFromDupReader, dupReaderIndex);
                        mergeHeap.offer(new MergeElement(nextDupKey, nextRowFromDupReader, dupReaderIndex));
                    }
                }
            }
        }
    }
    
    /**
     * 从行数据中提取 RowKey
     * 
     * @param row 数据行
     * @param readerIndex 数据源索引（用于无主键表的排序）
     */
    private RowKey extractKey(Row row, int readerIndex) {
        if (schema.hasPrimaryKey()) {
            return RowKey.fromRow(row, schema);
        } else {
            // 无主键表：生成一个伪键用于排序
            // 使用 readerIndex 和序列号组合，保证稳定排序
            long seq = sequenceCounter++;
            String pseudoKey = String.format("%08d-%012d", readerIndex, seq);
            return new RowKey(pseudoKey.getBytes());
        }
    }
    
    @Override
    public void close() {
        // 清理资源
        iterators.clear();
        mergeHeap.clear();
    }
    
    /**
     * 归并元素
     */
    private static class MergeElement {
        final RowKey key;
        final Row row;
        final int readerIndex;
        
        MergeElement(RowKey key, Row row, int readerIndex) {
            this.key = key;
            this.row = row;
            this.readerIndex = readerIndex;
        }
    }
    
    /**
     * 批量读取所有数据（内存模式）
     * 
     * @param schema 表结构
     * @param sstablePaths SSTable 文件路径列表
     * @return 归并去重后的所有数据
     */
    public static List<Row> readAll(Schema schema, List<String> sstablePaths) throws IOException {
        List<Row> result = new ArrayList<>();
        try (MergeSortedReader reader = new MergeSortedReader(schema, sstablePaths)) {
            while (reader.hasNext()) {
                result.add(reader.next());
            }
        }
        return result;
    }
    
    /**
     * 批量读取所有数据（基于数据源）
     * 
     * @param schema 表结构
     * @param dataSources 数据源列表
     * @return 归并去重后的所有数据
     */
    public static List<Row> readAllFromIterators(Schema schema, List<Iterator<Row>> dataSources) throws IOException {
        List<Row> result = new ArrayList<>();
        try (MergeSortedReader reader = fromIterators(schema, dataSources)) {
            while (reader.hasNext()) {
                result.add(reader.next());
            }
        }
        return result;
    }
}
