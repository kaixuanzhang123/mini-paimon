package com.mini.paimon.flink.source;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.CatalogLoader;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.flink.table.FlinkRowConverter;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableRead;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;

public class PaimonSource extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private final ObjectPath tablePath;
    private final Map<String, String> options;
    private final RowType rowType;

    private volatile boolean isRunning = true;
    private transient Catalog catalog;
    private transient Table table;

    public PaimonSource(ObjectPath tablePath, Map<String, String> options, RowType rowType) {
        this.tablePath = tablePath;
        this.options = options;
        this.rowType = rowType;
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
        TableRead tableRead = table.newRead();
        com.mini.paimon.table.TableScan.Plan plan = table.newScan().plan();
        
        List<Row> rows = tableRead.read(plan);
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
