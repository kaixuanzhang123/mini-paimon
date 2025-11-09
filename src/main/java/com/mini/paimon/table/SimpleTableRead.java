package com.mini.paimon.table;

import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.List;

/**
 * 简单的 TableRead 实现
 * 委托给 DataTableScan 和 DataTableRead
 */
public class SimpleTableRead implements TableRead {
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final Identifier identifier;
    private final DataTableRead delegate;
    
    public SimpleTableRead(Schema schema, PathFactory pathFactory, Identifier identifier) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.delegate = new DataTableRead(schema);
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
        // 创建一个 DataTableScan 来执行实际的扫描
        DataTableScan scan = new DataTableScan(pathFactory, 
            identifier.getDatabase(), identifier.getTable(), schema);
        
        // 使用快照ID进行扫描
        if (plan != null && plan.snapshot() != null) {
            scan.withSnapshot(plan.snapshot().getId());
        }
        
        DataTableScan.Plan dataPlan = scan.plan();
        return delegate.read(dataPlan);
    }
}
