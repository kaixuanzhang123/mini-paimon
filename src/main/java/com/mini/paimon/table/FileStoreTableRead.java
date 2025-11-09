package com.mini.paimon.table;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FileStoreTable 读取器实现
 * 委托给 DataTableRead 实现
 */
public class FileStoreTableRead implements TableRead {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreTableRead.class);
    
    private final DataTableRead delegate;
    
    public FileStoreTableRead(Schema schema, PathFactory pathFactory, String database, String table) {
        this.delegate = new DataTableRead(schema, pathFactory, database, table);
    }
    
    @Override
    public TableRead withProjection(Projection projection) {
        delegate.withProjection(projection);
        return this;
    }
    
    @Override
    public TableRead withFilter(Predicate predicate) {
        delegate.withFilter(predicate);
        return this;
    }
    
    @Override
    public List<Row> read(TableScan.Plan plan) throws IOException {
        if (plan == null || plan.snapshot() == null) {
            return new ArrayList<>();
        }
        
        // 将 TableScan.Plan 转换为 DataTableScan.Plan
        Snapshot snapshot = plan.snapshot();
        List<ManifestEntry> files = plan.files();
        
        DataTableScan.Plan delegatePlan = new DataTableScan.Plan(snapshot, files);
        
        return delegate.read(delegatePlan);
    }
}
