package com.mini.paimon.flink.sink;

import com.mini.paimon.flink.table.PaimonSinkFunction;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Map;

public class PaimonDynamicTableSink implements DynamicTableSink, SupportsPartitioning {

    private final ResolvedCatalogTable catalogTable;
    private final DynamicTableFactory.Context context;

    public PaimonDynamicTableSink(ResolvedCatalogTable catalogTable, DynamicTableFactory.Context context) {
        this.catalogTable = catalogTable;
        this.context = context;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        return SinkFunctionProvider.of(
            new PaimonSinkFunction(
                context.getObjectIdentifier().toObjectPath(),
                catalogTable.getOptions()
            )
        );
    }

    @Override
    public DynamicTableSink copy() {
        return new PaimonDynamicTableSink(catalogTable, context);
    }

    @Override
    public String asSummaryString() {
        return "PaimonDynamicTableSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return false;
    }
}
