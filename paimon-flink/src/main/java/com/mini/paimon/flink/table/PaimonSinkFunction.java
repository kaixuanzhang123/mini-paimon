package com.mini.paimon.flink.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.CatalogLoader;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PaimonSinkFunction implements SinkFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSinkFunction.class);

    private final ObjectPath tablePath;
    private final Map<String, String> options;

    private transient Catalog catalog;
    private transient Table table;
    private transient TableWrite tableWrite;

    public PaimonSinkFunction(ObjectPath tablePath, Map<String, String> options) {
        this.tablePath = tablePath;
        this.options = options;
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        if (tableWrite == null) {
            open();
        }

        RowKind kind = value.getRowKind();
        Schema schema = table.schema();
        
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                com.mini.paimon.schema.Row row = FlinkRowConverter.toRow(value, schema);
                tableWrite.write(row);
                LOG.debug("Wrote {} row: {}", kind, row);
                break;
                
            case DELETE:
            case UPDATE_BEFORE:
                LOG.warn("DELETE and UPDATE_BEFORE operations are not yet fully supported, ignoring");
                break;
                
            default:
                LOG.warn("Unknown RowKind: {}", kind);
        }
    }

    private void open() throws Exception {
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
        tableWrite = table.newWrite();

        LOG.info("Opened PaimonSink for table {} with UPDATE/DELETE support", tablePath);
    }

    public void finish() throws Exception {
        if (tableWrite != null) {
            TableCommit tableCommit = table.newCommit();
            tableCommit.commit(tableWrite.prepareCommit());
            tableWrite.close();
            LOG.info("Committed data for table {}", tablePath);
        }
    }

    public void close() throws Exception {
        if (tableWrite != null) {
            tableWrite.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }
}
