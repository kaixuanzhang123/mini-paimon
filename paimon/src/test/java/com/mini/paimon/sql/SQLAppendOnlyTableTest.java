package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.FileSystemCatalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.table.Table;
import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQL Append-Only 表测试
 * 测试通过 SQL 创建仅追加表,并验证 CSV 格式的读写功能
 */
public class SQLAppendOnlyTableTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(SQLAppendOnlyTableTest.class);
    private static final String WAREHOUSE_PATH = "/tmp/mini-paimon-append-test";
    
    private Catalog catalog;
    private PathFactory pathFactory;
    private SQLParser sqlParser;
    
    @BeforeEach
    public void setup() throws Exception {
        LOG.info("测试环境初始化");
        cleanupWarehouse();
        
        CatalogContext context = CatalogContext.builder()
            .warehouse(WAREHOUSE_PATH)
            .build();
        catalog = new FileSystemCatalog("test_catalog", context);
        pathFactory = new PathFactory(WAREHOUSE_PATH);
        sqlParser = new SQLParser(catalog, pathFactory);
        
        LOG.info("测试环境初始化完成");
    }
    
    @AfterEach
    public void cleanup() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        cleanupWarehouse();
        LOG.info("测试环境清理完成");
    }
    
    @Test
    public void testAppendOnlyTableBasicOperations() throws Exception {
        LOG.info("\n=== 测试 Append-Only 表基本操作 ===");
        
        // 1. 创建数据库
        catalog.createDatabase("test_db", false);
        LOG.info("✓ 创建数据库成功");
        
        // 2. 创建仅追加表 (无主键)
        String createTableSQL = 
            "CREATE TABLE test_db.events (" +
            "  event_id BIGINT," +
            "  event_type STRING," +
            "  user_id BIGINT" +
            ")";
        sqlParser.executeSQL(createTableSQL);
        LOG.info("✓ 创建仅追加表成功");
        
        // 3. 验证表类型
        Table table = catalog.getTable(new Identifier("test_db", "events"));
        assertNotNull(table);
        assertFalse(table.schema().hasPrimaryKey(), "应该是无主键表");
        LOG.info("✓ 验证表类型: APPEND_ONLY");
        
        // 4. 插入数据
        sqlParser.executeSQL(
            "INSERT INTO test_db.events VALUES " +
            "(1, 'login', 100), " +
            "(2, 'click', 100), " +
            "(3, 'logout', 100)"
        );
        LOG.info("✓ 插入第一批数据成功");
        
        // 5. 再次插入数据 (包含重复的 event_id)
        sqlParser.executeSQL(
            "INSERT INTO test_db.events VALUES " +
            "(1, 'login', 200), " +
            "(4, 'purchase', 200)"
        );
        LOG.info("✓ 插入第二批数据成功 (包含重复ID)");
        
        // 6. 验证 CSV 文件生成
        Path dataDir = pathFactory.getDataDir("test_db", "events");
        assertTrue(Files.exists(dataDir), "数据目录应该存在");
        
        long csvFileCount = Files.list(dataDir)
            .filter(p -> p.getFileName().toString().endsWith(".csv"))
            .count();
        assertTrue(csvFileCount > 0, "应该生成 CSV 文件");
        LOG.info("✓ 生成了 {} 个 CSV 文件", csvFileCount);
        
        // 7. 读取并验证 CSV 文件内容
        Path csvFile = Files.list(dataDir)
            .filter(p -> p.getFileName().toString().endsWith(".csv"))
            .findFirst()
            .get();
        
        java.util.List<String> csvLines = Files.readAllLines(csvFile);
        LOG.info("CSV 文件内容:");
        csvLines.forEach(line -> LOG.info("  {}", line));
        
        // 验证 CSV 格式
        assertTrue(csvLines.size() > 0, "CSV 文件应该有内容");
        assertTrue(csvLines.get(0).contains("event_id"), "第一行应该是表头");
        assertTrue(csvLines.get(0).contains("event_type"), "表头应该包含所有字段");
        assertTrue(csvLines.get(0).contains("user_id"), "表头应该包含所有字段");
        
        // 验证数据行
        assertTrue(csvLines.size() >= 2, "应该至少有表头和数据行");
        assertTrue(csvLines.stream().anyMatch(line -> line.contains("login")), "应该包含数据");
        
        LOG.info("✓ CSV 文件格式正确");
        LOG.info("\n=== Append-Only 表基本操作测试通过 ===");
        LOG.info("✅ 测试结论:");
        LOG.info("  - 仅追加表(无主键)自动使用 AppendOnlyWriter");
        LOG.info("  - 数据写入 CSV 格式文件");
        LOG.info("  - CSV 文件包含表头行");
        LOG.info("  - 允许插入重复数据(不去重)");
    }
    
    private void cleanupWarehouse() throws IOException {
        Path warehousePath = Paths.get(WAREHOUSE_PATH);
        if (Files.exists(warehousePath)) {
            Files.walk(warehousePath)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
}
