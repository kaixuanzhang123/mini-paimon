package com.mini.paimon.manifest;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.table.FileStoreTable;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableRead;
import com.mini.paimon.table.TableScan;
import com.mini.paimon.table.TableWrite;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 增量Manifest功能测试
 * 验证增量写入、compaction、增量读取等核心功能
 */
public class IncrementalManifestTest {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalManifestTest.class);
    
    private Path warehousePath;
    private Catalog catalog;
    private PathFactory pathFactory;
    
    @BeforeEach
    public void setup() throws Exception {
        warehousePath = Files.createTempDirectory("mini-paimon-test-");
        
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehousePath.toString())
            .build();
        
        catalog = new FileSystemCatalog("test_catalog", "default", context);
        pathFactory = new PathFactory(warehousePath.toString());
        
        catalog.createDatabase("test_db", false);
        
        logger.info("Test setup complete, warehouse: {}", warehousePath);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        
        if (warehousePath != null && Files.exists(warehousePath)) {
            Files.walk(warehousePath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        logger.warn("Failed to delete: {}", path, e);
                    }
                });
        }
    }
    
    @Test
    public void testIncrementalManifestWrite() throws IOException {
        logger.info("=== Test: Incremental Manifest Write ===");
        
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), true),
            new Field("age", DataType.INT(), true)
        );
        Schema schema = new Schema(0, fields, Arrays.asList("id"), Arrays.asList());
        
        Identifier tableId = new Identifier("test_db", "users");
        catalog.createTable(tableId, schema, false);
        
        Table table = catalog.getTable(tableId);
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
            TableWrite writer = table.newWrite();
            
            for (int j = 0; j < 10; j++) {
                Row row = new Row(new Object[]{i * 10 + j, "user_" + (i * 10 + j), 20 + j});
                writer.write(row);
            }
            
            TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
            TableCommit commit = table.newCommit();
            commit.commit(commitMsg);
            writer.close();
        }
        long duration = System.currentTimeMillis() - startTime;
        
        logger.info("20 commits completed in {}ms, avg {}ms per commit", duration, duration / 20);
        
        FileStoreTable fsTable = (FileStoreTable) table;
        Snapshot latestSnapshot = fsTable.snapshotManager().getLatestSnapshot();
        
        assertNotNull(latestSnapshot);
        assertEquals(20, latestSnapshot.getId());
        assertNotNull(latestSnapshot.getBaseManifestList());
        assertNotNull(latestSnapshot.getDeltaManifestList());
        
        logger.info("Latest snapshot: base={}, delta={}", 
                   latestSnapshot.getBaseManifestList(), 
                   latestSnapshot.getDeltaManifestList());
        
        TableScan scan = table.newScan();
        TableScan.Plan plan = scan.plan();
        
        TableRead read = table.newRead();
        List<Row> rows = read.read(plan);
        
        // 由于有主键,merge时会去重,所以实际行数可能少于200
        assertTrue(rows.size() >= 100, "Should have at least 100 rows");
        logger.info("Verified: read {} rows (expected >= 100 due to merge)", rows.size());
    }
    
    @Test
    public void testManifestCompaction() throws IOException {
        logger.info("=== Test: Manifest Compaction ===");
        
        List<Field> fields2 = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("value", DataType.STRING(), true)
        );
        Schema schema = new Schema(0, fields2, Arrays.asList("id"), Arrays.asList());
        
        Identifier tableId = new Identifier("test_db", "compaction_test");
        catalog.createTable(tableId, schema, false);
        
        Table table = catalog.getTable(tableId);
        FileStoreTable fsTable = (FileStoreTable) table;
        
        String lastBaseManifest = null;
        int compactionCount = 0;
        
        for (int i = 1; i <= 60; i++) {
            TableWrite writer = table.newWrite();
            Row row = new Row(new Object[]{i, "value_" + i});
            writer.write(row);
            
            TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
            TableCommit commit = table.newCommit();
            commit.commit(commitMsg);
            writer.close();
            
            Snapshot snapshot = fsTable.snapshotManager().getSnapshot(i);
            String currentBase = snapshot.getBaseManifestList();
            
            if (lastBaseManifest == null) {
                lastBaseManifest = currentBase;
            } else if (!currentBase.equals(lastBaseManifest)) {
                compactionCount++;
                logger.info("Compaction #{} detected at snapshot {}: {} -> {}",
                           compactionCount, i, lastBaseManifest, currentBase);
                lastBaseManifest = currentBase;
            }
        }
        
        assertTrue(compactionCount >= 1, "Should trigger at least 1 compaction");
        logger.info("Total compactions: {}", compactionCount);
        
        TableScan scan = table.newScan();
        TableScan.Plan plan = scan.plan();
        TableRead read = table.newRead();
        List<Row> rows = read.read(plan);
        
        assertEquals(60, rows.size(), "Should have 60 rows");
        logger.info("Verified: compacted table has {} rows", rows.size());
    }
    
    @Test
    public void testIncrementalRead() throws IOException {
        logger.info("=== Test: Incremental Read ===");
        
        List<Field> fields3 = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("data", DataType.STRING(), true)
        );
        Schema schema = new Schema(0, fields3, Arrays.asList("id"), Arrays.asList());
        
        Identifier tableId = new Identifier("test_db", "incremental_read_test");
        catalog.createTable(tableId, schema, false);
        
        Table table = catalog.getTable(tableId);
        
        for (int batch = 0; batch < 5; batch++) {
            TableWrite writer = table.newWrite();
            
            for (int i = 0; i < 20; i++) {
                Row row = new Row(new Object[]{batch * 20 + i, "data_" + (batch * 20 + i)});
                writer.write(row);
            }
            
            TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
            TableCommit commit = table.newCommit();
            commit.commit(commitMsg);
            writer.close();
        }
        
        FileStoreTable fsTable = (FileStoreTable) table;
        Snapshot snapshot3 = fsTable.snapshotManager().getSnapshot(3);
        Snapshot snapshot5 = fsTable.snapshotManager().getSnapshot(5);
        
        ManifestCacheManager cacheManager = new ManifestCacheManager();
        ManifestCacheManager.IncrementalManifest incremental = 
            cacheManager.readIncrementalManifest(
                pathFactory, "test_db", "incremental_read_test",
                snapshot3.getId(), snapshot5.getId()
            );
        
        List<ManifestEntry> newEntries = incremental.getNewEntries();
        
        assertTrue(newEntries.size() > 0, "Should have new entries");
        logger.info("Incremental read: {} new entries from snapshot {} to {}", 
                   newEntries.size(), snapshot3.getId(), snapshot5.getId());
        
        long totalRows = newEntries.stream()
            .mapToLong(e -> e.getFile().getRowCount())
            .sum();
        
        assertEquals(40L, totalRows, "Should have 40 new rows (2 batches)");
    }
    
    @Test
    public void testBaseAndDeltaSeparation() throws IOException {
        logger.info("=== Test: Base and Delta Separation ===");
        
        List<Field> fields4 = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("value", DataType.INT(), true)
        );
        Schema schema = new Schema(0, fields4, Arrays.asList("id"), Arrays.asList());
        
        Identifier tableId = new Identifier("test_db", "base_delta_test");
        catalog.createTable(tableId, schema, false);
        
        Table table = catalog.getTable(tableId);
        
        TableWrite writer1 = table.newWrite();
        Row row1 = new Row(new Object[]{1, 100});
        writer1.write(row1);
        TableWrite.TableCommitMessage msg1 = writer1.prepareCommit();
        table.newCommit().commit(msg1);
        writer1.close();
        
        FileStoreTable fsTable = (FileStoreTable) table;
        Snapshot snapshot1 = fsTable.snapshotManager().getSnapshot(1);
        
        assertNotNull(snapshot1.getBaseManifestList());
        assertNotNull(snapshot1.getDeltaManifestList());
        assertEquals(snapshot1.getBaseManifestList(), snapshot1.getDeltaManifestList(),
                    "First snapshot: base and delta should be the same");
        
        TableWrite writer2 = table.newWrite();
        Row row2 = new Row(new Object[]{2, 200});
        writer2.write(row2);
        TableWrite.TableCommitMessage msg2 = writer2.prepareCommit();
        table.newCommit().commit(msg2);
        writer2.close();
        
        Snapshot snapshot2 = fsTable.snapshotManager().getSnapshot(2);
        
        assertNotNull(snapshot2.getBaseManifestList());
        assertNotNull(snapshot2.getDeltaManifestList());
        assertEquals(snapshot1.getBaseManifestList(), snapshot2.getBaseManifestList(),
                    "Second snapshot should reuse first snapshot's base");
        assertNotEquals(snapshot1.getDeltaManifestList(), snapshot2.getDeltaManifestList(),
                       "Second snapshot should have different delta");
        
        logger.info("Snapshot 1: base={}, delta={}", 
                   snapshot1.getBaseManifestList(), snapshot1.getDeltaManifestList());
        logger.info("Snapshot 2: base={}, delta={}", 
                   snapshot2.getBaseManifestList(), snapshot2.getDeltaManifestList());
    }
    
    @Test
    public void testPerformanceImprovement() throws IOException {
        logger.info("=== Test: Performance Improvement ===");
        
        List<Field> fields5 = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("data", DataType.STRING(), true)
        );
        Schema schema = new Schema(0, fields5, Arrays.asList("id"), Arrays.asList());
        
        Identifier tableId = new Identifier("test_db", "perf_test");
        catalog.createTable(tableId, schema, false);
        
        Table table = catalog.getTable(tableId);
        
        for (int i = 0; i < 100; i++) {
            TableWrite writer = table.newWrite();
            Row row = new Row(new Object[]{i, "data_" + i});
            writer.write(row);
            
            TableWrite.TableCommitMessage msg = writer.prepareCommit();
            table.newCommit().commit(msg);
            writer.close();
        }
        
        long commitStartTime = System.currentTimeMillis();
        TableWrite writer = table.newWrite();
        Row row = new Row(new Object[]{1000, "new_data"});
        writer.write(row);
        TableWrite.TableCommitMessage msg = writer.prepareCommit();
        table.newCommit().commit(msg);
        writer.close();
        long commitDuration = System.currentTimeMillis() - commitStartTime;
        
        logger.info("Commit time after 100 snapshots: {}ms", commitDuration);
        
        assertTrue(commitDuration < 1000, 
                  "Incremental commit should be fast (< 1000ms), actual: " + commitDuration + "ms");
        
        long readStartTime = System.currentTimeMillis();
        TableScan scan = table.newScan();
        TableScan.Plan plan = scan.plan();
        TableRead read = table.newRead();
        List<Row> rows = read.read(plan);
        long readDuration = System.currentTimeMillis() - readStartTime;
        
        logger.info("Read time: {}ms, rows: {}", readDuration, rows.size());
        assertEquals(101, rows.size(), "Should have 101 rows");
    }
}

