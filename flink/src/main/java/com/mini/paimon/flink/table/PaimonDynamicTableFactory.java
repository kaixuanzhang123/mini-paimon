package com.mini.paimon.flink.table;

import com.mini.paimon.flink.sink.PaimonDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

public class PaimonDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "mini-paimon";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        return new PaimonDynamicTableSource(catalogTable, context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        return new PaimonDynamicTableSink(catalogTable, context);
    }
}
