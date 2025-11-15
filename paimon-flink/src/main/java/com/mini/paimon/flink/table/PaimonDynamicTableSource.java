package com.mini.paimon.flink.table;

import com.mini.paimon.flink.source.PaimonSource;
import com.mini.paimon.table.Predicate;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class PaimonDynamicTableSource implements ScanTableSource, SupportsFilterPushDown {

    private final ResolvedCatalogTable catalogTable;
    private final DynamicTableFactory.Context context;
    private List<ResolvedExpression> filters;

    public PaimonDynamicTableSource(ResolvedCatalogTable catalogTable, DynamicTableFactory.Context context) {
        this.catalogTable = catalogTable;
        this.context = context;
        this.filters = new ArrayList<>();
    }
    
    private PaimonDynamicTableSource(ResolvedCatalogTable catalogTable, 
                                     DynamicTableFactory.Context context,
                                     List<ResolvedExpression> filters) {
        this.catalogTable = catalogTable;
        this.context = context;
        this.filters = filters;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        RowType rowType = (RowType) catalogTable.getResolvedSchema().toPhysicalRowDataType().getLogicalType();
        
        Predicate predicate = null;
        if (!filters.isEmpty()) {
            predicate = FlinkFilterConverter.convert(filters, catalogTable.getResolvedSchema());
        }
        
        return SourceFunctionProvider.of(
            new PaimonSource(
                context.getObjectIdentifier().toObjectPath(),
                catalogTable.getOptions(),
                rowType,
                predicate
            ), 
            true
        );
    }

    @Override
    public DynamicTableSource copy() {
        return new PaimonDynamicTableSource(catalogTable, context, filters);
    }
    
    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = new ArrayList<>(filters);
        return Result.of(new ArrayList<>(), new ArrayList<>(filters));
    }

    @Override
    public String asSummaryString() {
        return "PaimonDynamicTableSource";
    }
}
