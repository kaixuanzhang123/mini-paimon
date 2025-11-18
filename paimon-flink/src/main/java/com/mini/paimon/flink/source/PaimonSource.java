package com.mini.paimon.flink.source;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.CatalogLoader;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.flink.table.FlinkRowConverter;
import com.mini.paimon.schema.Row;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableRead;
import com.mini.paimon.table.TableScan;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class PaimonSource extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSource.class);
    
    private final ObjectPath tablePath;
    private final Map<String, String> options;
    private final RowType rowType;
    private final Predicate predicate;

    private volatile boolean isRunning = true;
    private transient Catalog catalog;
    private transient Table table;

    public PaimonSource(ObjectPath tablePath, Map<String, String> options, RowType rowType) {
        this(tablePath, options, rowType, null);
    }
    
    public PaimonSource(ObjectPath tablePath, Map<String, String> options, RowType rowType, Predicate predicate) {
        this.tablePath = tablePath;
        this.options = options;
        this.rowType = rowType;
        this.predicate = predicate;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        String warehouse = options.getOrDefault("warehouse", "warehouse");

        // 使用 CatalogLoader 通过 SPI 机制加载 Catalog
        CatalogContext catalogContext = CatalogContext.builder()
            .warehouse(warehouse)
            .option("catalog.name", "paimon")
            .option("catalog.default-database", tablePath.getDatabaseName())
            .build();

        catalog = CatalogLoader.load("filesystem", catalogContext);

        Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
        table = catalog.getTable(identifier);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        TableScan scan = table.newScan();
        
        if (predicate != null) {
            LOG.info("Applying filter predicate: {}", predicate);
            scan = scan.withFilter(predicate);
        }
        
        TableScan.Plan plan = scan.plan();
        
        LOG.info("Scan plan generated with {} files", plan.files().size());
        
        TableRead tableRead = table.newRead();
        if (predicate != null) {
            LOG.info("Applying filter predicate to TableRead: {}", predicate);
            tableRead = tableRead.withFilter(predicate);
        }
        List<Row> rows = tableRead.read(plan);
        
        LOG.info("Read {} rows from table", rows.size());
        
        for (Row row : rows) {
            if (!isRunning) {
                break;
            }
            RowData rowData = FlinkRowConverter.toRowData(row, table.schema());
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(rowData);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(rowType);
    }
}
