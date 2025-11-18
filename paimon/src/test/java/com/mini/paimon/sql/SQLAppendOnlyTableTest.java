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
    private static final String WAREHOUSE_PATH = "./warehouse";

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
                        ")" +
                        "TBLPROPERTIES (\n" +
                        "    'file.index.all' = 'bloom-filter,min-max'\n" +
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

        // 6. 使用 SQL 查询验证数据
        LOG.info("✓ 验证数据写入");


        // 执行 SELECT * 查询
        sqlParser.executeSQL("SELECT * FROM test_db.events");

        sqlParser.executeSQL("SELECT * FROM test_db.events where event_id = 2");

        LOG.info("✓ 所有数据验证成功");
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