package com.mini.paimon.flink.table;

import com.mini.paimon.flink.source.PaimonSource;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;

public class PaimonDynamicTableSource implements ScanTableSource {

    private final ResolvedCatalogTable catalogTable;
    private final DynamicTableFactory.Context context;

    public PaimonDynamicTableSource(ResolvedCatalogTable catalogTable, DynamicTableFactory.Context context) {
        this.catalogTable = catalogTable;
        this.context = context;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) catalogTable.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
        return SourceFunctionProvider.of(
            new PaimonSource(
                context.getObjectIdentifier().toObjectPath(),
                catalogTable.getOptions(),
                rowType
            ), 
            true
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new PaimonDynamicTableSource(catalogTable, context);
    }

    @Override
    public String asSummaryString() {
        return "PaimonDynamicTableSource";
    }
}
