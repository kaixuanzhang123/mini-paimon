package com.mini.paimon.spark.catalog;

import com.mini.paimon.spark.sink.PaimonBatchWrite;
import com.mini.paimon.spark.source.PaimonBatch;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Table;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class SparkTable implements SupportsRead, SupportsWrite {

    private final Table paimonTable;
    private final String warehouse;

    public SparkTable(Table paimonTable, String warehouse) {
        this.paimonTable = paimonTable;
        this.warehouse = warehouse;
    }

    @Override
    public String name() {
        return paimonTable.identifier().getFullName();
    }

    @Override
    public StructType schema() {
        return SparkSchemaConverter.toSparkSchema(paimonTable.schema());
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        capabilities.add(TableCapability.BATCH_WRITE);
        return capabilities;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new PaimonScanBuilder(paimonTable, warehouse);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new PaimonWriteBuilder(paimonTable, info, warehouse);
    }

    private static class PaimonScanBuilder implements ScanBuilder, SupportsPushDownFilters {
        private static final Logger LOG = LoggerFactory.getLogger(PaimonScanBuilder.class);
        
        private final Table paimonTable;
        private final String warehouse;
        private Filter[] pushedFilters = new Filter[0];

        public PaimonScanBuilder(Table paimonTable, String warehouse) {
            this.paimonTable = paimonTable;
            this.warehouse = warehouse;
        }

        @Override
        public org.apache.spark.sql.connector.read.Scan build() {
            Predicate predicate = SparkFilterConverter.convert(pushedFilters, paimonTable.schema());
            if (predicate != null) {
                LOG.info("Built scan with filter predicate: {}", predicate);
            }
            return new PaimonScan(paimonTable, warehouse, predicate);
        }
        
        @Override
        public Filter[] pushFilters(Filter[] filters) {
            this.pushedFilters = filters;
            return filters;
        }
        
        @Override
        public Filter[] pushedFilters() {
            return pushedFilters;
        }
    }

    private static class PaimonScan implements org.apache.spark.sql.connector.read.Scan {
        private final Table paimonTable;
        private final String warehouse;
        private final Predicate predicate;

        public PaimonScan(Table paimonTable, String warehouse, Predicate predicate) {
            this.paimonTable = paimonTable;
            this.warehouse = warehouse;
            this.predicate = predicate;
        }

        @Override
        public StructType readSchema() {
            return SparkSchemaConverter.toSparkSchema(paimonTable.schema());
        }

        @Override
        public org.apache.spark.sql.connector.read.Batch toBatch() {
            return new PaimonBatch(paimonTable, warehouse, predicate);
        }
    }

    private static class PaimonWriteBuilder implements WriteBuilder {
        private final Table paimonTable;
        private final LogicalWriteInfo info;
        private final String warehouse;

        public PaimonWriteBuilder(Table paimonTable, LogicalWriteInfo info, String warehouse) {
            this.paimonTable = paimonTable;
            this.info = info;
            this.warehouse = warehouse;
        }

        @Override
        public org.apache.spark.sql.connector.write.BatchWrite buildForBatch() {
            String database = paimonTable.identifier().getDatabase();
            String tableName = paimonTable.identifier().getTable();
            return new PaimonBatchWrite(warehouse, database, tableName, info.schema());
        }
    }
}

