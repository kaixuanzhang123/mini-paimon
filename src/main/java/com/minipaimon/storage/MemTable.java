package com.minipaimon.storage;

import com.google.common.collect.Maps;
import com.minipaimon.metadata.Row;
import com.minipaimon.metadata.RowKey;
import com.minipaimon.metadata.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 内存表实现
 * 使用 ConcurrentSkipListMap 实现有序内存表
 * 支持基本的 put/get 操作
 */
public class MemTable {
    private static final Logger logger = LoggerFactory.getLogger(MemTable.class);

    /** 有序内存表，按键排序存储 */
    private final ConcurrentSkipListMap<RowKey, Row> memTable;
    
    /** 表结构 */
    private final Schema schema;
    
    /** 当前内存表大小（字节） */
    private volatile long size;
    
    /** 最大大小限制（默认64MB） */
    private final long maxSize;
    
    /** 序列号，用于SSTable文件命名 */
    private final long sequenceNumber;

    public MemTable(Schema schema, long sequenceNumber) {
        this(schema, sequenceNumber, 64 * 1024 * 1024); // 默认64MB
    }

    public MemTable(Schema schema, long sequenceNumber, long maxSize) {
        this.memTable = new ConcurrentSkipListMap<>();
        this.schema = schema;
        this.size = 0;
        this.maxSize = maxSize;
        this.sequenceNumber = sequenceNumber;
    }

    /**
     * 向内存表中插入数据
     * 
     * @param row 数据行
     * @return 插入的键值对大小（字节）
     */
    public int put(Row row) {
        // 验证数据行
        row.validate(schema);
        
        // 生成主键
        RowKey key = RowKey.fromRow(row, schema);
        
        // 计算行大小
        int rowSize = calculateRowSize(row, key);
        
        // 插入到内存表
        memTable.put(key, row);
        
        // 更新大小
        size += rowSize;
        
        logger.debug("Put row with key: {}, size: {} bytes", key, rowSize);
        
        return rowSize;
    }

    /**
     * 从内存表中获取数据
     * 
     * @param key 主键
     * @return 数据行，如果不存在返回null
     */
    public Row get(RowKey key) {
        return memTable.get(key);
    }

    /**
     * 获取内存表中的所有数据
     * 
     * @return 包含所有数据行的Map
     */
    public Map<RowKey, Row> getAllData() {
        return Maps.newHashMap(memTable);
    }

    /**
     * 计算行的大小（近似值）
     */
    private int calculateRowSize(Row row, RowKey key) {
        // 简化计算：主键大小 + 行大小（每个字段值的大小估算）
        int size = key.size();
        
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object value = row.getValue(i);
            if (value != null) {
                if (value instanceof String) {
                    size += ((String) value).getBytes().length;
                } else if (value instanceof Integer) {
                    size += 4;
                } else if (value instanceof Long) {
                    size += 8;
                } else if (value instanceof Boolean) {
                    size += 1;
                }
            }
        }
        
        return size;
    }

    /**
     * 检查内存表是否已满
     */
    public boolean isFull() {
        return size >= maxSize;
    }

    /**
     * 获取内存表大小
     */
    public long getSize() {
        return size;
    }

    /**
     * 获取最大大小限制
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * 获取序列号
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * 获取内存表中的所有条目
     */
    public Map<RowKey, Row> getEntries() {
        return Maps.newHashMap(memTable);
    }

    /**
     * 检查内存表是否为空
     */
    public boolean isEmpty() {
        return memTable.isEmpty();
    }

    /**
     * 获取内存表中的条目数量
     */
    public int size() {
        return memTable.size();
    }

    @Override
    public String toString() {
        return "MemTable{" +
                "size=" + size +
                ", maxSize=" + maxSize +
                ", entryCount=" + memTable.size() +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}