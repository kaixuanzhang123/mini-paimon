package com.mini.paimon.table;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.read.ParallelDataReader;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FileStoreTable 读取器实现
 * 支持串行和并行两种读取模式
 */
public class FileStoreTableRead implements TableRead {
    private static final Logger logger = LoggerFactory.getLogger(FileStoreTableRead.class);
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final DataTableRead delegate;
    
    // 并行读取配置
    private boolean enableParallelRead = false;
    private int parallelism = Runtime.getRuntime().availableProcessors();
    
    private Projection projection;
    private Predicate predicate;
    
    public FileStoreTableRead(Schema schema, PathFactory pathFactory, String database, String table) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.delegate = new DataTableRead(schema, pathFactory, database, table);
    }
    
    /**
     * 启用并行读取
     */
    public FileStoreTableRead withParallelRead(boolean enable) {
        this.enableParallelRead = enable;
        return this;
    }
    
    /**
     * 设置并行度
     */
    public FileStoreTableRead withParallelism(int parallelism) {
        this.parallelism = Math.max(1, parallelism);
        return this;
    }
    
    @Override
    public TableRead withProjection(Projection projection) {
        this.projection = projection;
        delegate.withProjection(projection);
        return this;
    }
    
    @Override
    public TableRead withFilter(Predicate predicate) {
        this.predicate = predicate;
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
        
        // 根据文件数量决定是否使用并行读取
        boolean shouldUseParallel = enableParallelRead && files.size() > 1;
        
        if (shouldUseParallel) {
            return parallelRead(snapshot, files);
        } else {
            DataTableScan.Plan delegatePlan = new DataTableScan.Plan(snapshot, files);
            return delegate.read(delegatePlan);
        }
    }
    
    /**
     * 并行读取数据
     */
    private List<Row> parallelRead(Snapshot snapshot, List<ManifestEntry> files) 
            throws IOException {
        logger.info("Using parallel read for {} files with parallelism {}", 
                   files.size(), parallelism);
        
        DataTableScan.Plan plan = new DataTableScan.Plan(snapshot, files);
        
        try (ParallelDataReader parallelReader = new ParallelDataReader(
                schema, pathFactory, database, table, parallelism)) {
            
            if (projection != null) {
                parallelReader.withProjection(projection);
            }
            if (predicate != null) {
                parallelReader.withFilter(predicate);
            }
            
            return parallelReader.read(plan);
        }
    }
}
