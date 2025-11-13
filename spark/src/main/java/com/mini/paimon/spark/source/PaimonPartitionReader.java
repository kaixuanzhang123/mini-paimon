package com.mini.paimon.spark.source;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.spark.table.SparkRowConverter;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableRead;
import com.mini.paimon.table.TableScan;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PaimonPartitionReader implements PartitionReader<InternalRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonPartitionReader.class);

    private final Table paimonTable;
    private Iterator<Row> rowIterator;
    private InternalRow currentRow;

    public PaimonPartitionReader(Table paimonTable) {
        this.paimonTable = paimonTable;
        initialize();
    }

    private void initialize() {
        try {
            TableScan scan = paimonTable.newScan();
            TableScan.Plan plan = scan.plan();

            if (plan == null || plan.files().isEmpty()) {
                rowIterator = java.util.Collections.emptyIterator();
                return;
            }

            TableRead read = paimonTable.newRead();
            List<Row> allRows = read.read(plan);

            rowIterator = allRows.iterator();
        } catch (Exception e) {
            LOG.error("Failed to initialize partition reader", e);
            rowIterator = java.util.Collections.emptyIterator();
        }
    }

    @Override
    public boolean next() throws IOException {
        if (rowIterator.hasNext()) {
            Row paimonRow = rowIterator.next();
            currentRow = SparkRowConverter.toInternalRow(paimonRow, paimonTable.schema());
            return true;
        }
        return false;
    }

    @Override
    public InternalRow get() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
    }
}

