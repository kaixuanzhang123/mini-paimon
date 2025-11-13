package com.mini.paimon.storage;

import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.metadata.TableType;

import java.util.*;

public class MergeEngine {
    
    private final Schema schema;
    private final TableType tableType;
    
    public MergeEngine(Schema schema, TableType tableType) {
        this.schema = schema;
        this.tableType = tableType;
    }
    
    public List<Row> merge(List<Iterator<Row>> sources) {
        if (tableType.isAppendOnly()) {
            return mergeAppendOnly(sources);
        } else {
            return mergePrimaryKey(sources);
        }
    }
    
    private List<Row> mergeAppendOnly(List<Iterator<Row>> sources) {
        List<Row> result = new ArrayList<>();
        for (Iterator<Row> source : sources) {
            while (source.hasNext()) {
                result.add(source.next());
            }
        }
        return result;
    }
    
    private List<Row> mergePrimaryKey(List<Iterator<Row>> sources) {
        if (sources.isEmpty()) {
            return Collections.emptyList();
        }
        
        Map<RowKey, Row> mergedData = new LinkedHashMap<>();
        
        for (Iterator<Row> source : sources) {
            while (source.hasNext()) {
                Row row = source.next();
                RowKey key = RowKey.fromRow(row, schema);
                mergedData.put(key, row);
            }
        }
        
        return new ArrayList<>(mergedData.values());
    }
    
    public List<Row> deduplicate(List<Row> rows) {
        if (tableType.isAppendOnly()) {
            return rows;
        }
        
        Map<RowKey, Row> deduped = new LinkedHashMap<>();
        for (Row row : rows) {
            RowKey key = RowKey.fromRow(row, schema);
            deduped.put(key, row);
        }
        
        return new ArrayList<>(deduped.values());
    }
}

