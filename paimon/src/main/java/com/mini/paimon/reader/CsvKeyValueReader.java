package com.mini.paimon.reader;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.storage.CsvReader;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV KeyValue Reader
 * 参考 Paimon 的 KeyValueFileReader 设计
 * 
 * 为 CSV 文件提供 KeyValueFileReader 接口实现
 * 注意: CSV 文件是 Append-Only 的，不支持基于 Key 的查询
 */
public class CsvKeyValueReader implements KeyValueFileReader {
    
    private final String filePath;
    private final Schema schema;
    private Predicate predicate;
    private Projection projection;
    
    public CsvKeyValueReader(String filePath, Schema schema) {
        this.filePath = filePath;
        this.schema = schema;
    }
    
    @Override
    public KeyValueFileReader withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    @Override
    public KeyValueFileReader withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    @Override
    public Row get(RowKey key) throws IOException {
        // CSV 文件不支持点查询
        // 需要全表扫描来查找匹配的行
        
        try (CsvReader reader = new CsvReader(schema, Paths.get(filePath))) {
            Row row;
            while ((row = reader.readRow()) != null) {
                // 如果表有主键，比较主键
                if (schema.hasPrimaryKey()) {
                    RowKey rowKey = row.extractKey(schema);
                    if (rowKey.equals(key)) {
                        return applyProjection(row);
                    }
                }
            }
        }
        
        return null;
    }
    
    @Override
    public RecordReader<Row> readRange(RowKey startKey, RowKey endKey) throws IOException {
        // CSV 文件不支持范围查询优化
        // 需要全表扫描并过滤
        
        List<Row> rows = new ArrayList<>();
        
        try (CsvReader reader = new CsvReader(schema, Paths.get(filePath))) {
            Row row;
            while ((row = reader.readRow()) != null) {
                // 应用过滤条件
                if (predicate != null && !predicate.test(row, schema)) {
                    continue;
                }
                
                // 如果表有主键，检查范围
                if (schema.hasPrimaryKey()) {
                    RowKey rowKey = row.extractKey(schema);
                    
                    if (startKey != null && rowKey.compareTo(startKey) < 0) {
                        continue;
                    }
                    
                    if (endKey != null && rowKey.compareTo(endKey) > 0) {
                        continue;
                    }
                }
                
                // 应用投影
                Row projectedRow = applyProjection(row);
                rows.add(projectedRow);
            }
        }
        
        return new ListRecordReader(rows);
    }
    
    @Override
    public RecordReader<Row> readAll() throws IOException {
        List<Row> rows = new ArrayList<>();
        
        try (CsvReader reader = new CsvReader(schema, Paths.get(filePath))) {
            Row row;
            while ((row = reader.readRow()) != null) {
                // 应用过滤条件
                if (predicate != null && !predicate.test(row, schema)) {
                    continue;
                }
                
                // 应用投影
                Row projectedRow = applyProjection(row);
                rows.add(projectedRow);
            }
        }
        
        return new ListRecordReader(rows);
    }
    
    /**
     * 应用投影
     */
    private Row applyProjection(Row row) {
        if (projection == null || projection.isAll()) {
            return row;
        }
        return projection.project(row, schema);
    }
    
    @Override
    public void close() throws IOException {
        // CSV Reader 在每次读取时都会打开和关闭
        // 这里不需要做任何事情
    }
}

