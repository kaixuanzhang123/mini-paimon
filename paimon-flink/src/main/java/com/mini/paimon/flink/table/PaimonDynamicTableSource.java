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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PaimonDynamicTableSource implements ScanTableSource, SupportsFilterPushDown {
    
    private static final Logger LOG = LoggerFactory.getLogger(PaimonDynamicTableSource.class);

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
        
        LOG.info("=== PaimonDynamicTableSource: Creating scan runtime provider ===");
        LOG.info("Table: {}", context.getObjectIdentifier());
        LOG.info("Filters count: {}", filters.size());
        
        Predicate predicate = null;
        if (!filters.isEmpty()) {
            LOG.info("Converting {} filters to Paimon predicate", filters.size());
            predicate = FlinkFilterConverter.convert(filters, catalogTable.getResolvedSchema());
            LOG.info("Converted predicate: {}", predicate);
        } else {
            LOG.info("No filters to apply");
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
        LOG.info("=== PaimonDynamicTableSource: Applying filters ===");
        LOG.info("Received {} filters from Flink optimizer", filters.size());
        for (int i = 0; i < filters.size(); i++) {
            LOG.info("Filter {}: {}", i, filters.get(i));
        }
        
        this.filters = new ArrayList<>(filters);
        
        // 返回所有过滤条件都接受（第一个列表为空表示没有不接受的），
        // 第二个列表是已接受的过滤条件
        Result result = Result.of(new ArrayList<>(), new ArrayList<>(filters));
        LOG.info("All filters accepted for pushdown");
        
        return result;
    }

    @Override
    public String asSummaryString() {
        return "PaimonDynamicTableSource";
    }
}
