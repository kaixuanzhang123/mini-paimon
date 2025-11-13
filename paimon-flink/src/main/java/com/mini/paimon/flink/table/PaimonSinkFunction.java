package com.mini.paimon.flink.table;

import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableWrite;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PaimonSinkFunction implements SinkFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonSinkFunction.class);

    private final ObjectPath tablePath;
    private final Map<String, String> options;

    private transient FileSystemCatalog catalog;
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

        com.mini.paimon.metadata.Row row = FlinkRowConverter.toRow(value, table.schema());
        tableWrite.write(row);
    }

    private void open() throws Exception {
        String warehouse = options.getOrDefault("warehouse", "warehouse");

        CatalogContext catalogContext = CatalogContext.builder()
            .warehouse(warehouse)
            .build();

        catalog = new FileSystemCatalog("paimon", tablePath.getDatabaseName(), catalogContext);

        Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
        table = catalog.getTable(identifier);
        tableWrite = table.newWrite();

        LOG.info("Opened PaimonSink for table {}", tablePath);
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
