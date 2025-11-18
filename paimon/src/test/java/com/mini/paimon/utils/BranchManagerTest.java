package com.mini.paimon.utils;

import com.mini.paimon.branch.BranchManager;
import com.mini.paimon.catalog.*;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * BranchManager 测试
 */
public class BranchManagerTest {
    
    private String warehouse;
    private Catalog catalog;
    private Identifier testTable;
    
    @Before
    public void setUp() throws Exception {
        // 创建临时测试目录
        warehouse = "/tmp/mini-paimon-branch-test-" + System.currentTimeMillis();
        
        // 初始化 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehouse)
            .option("catalog.name", "test")
            .option("catalog.default-database", "default")
            .build();
        catalog = new FileSystemCatalog("test", context);
        
        // 创建测试数据库和表
        catalog.createDatabase("test_db", true);
        
        testTable = new Identifier("test_db", "test_table");
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", com.mini.paimon.schema.DataType.INT(), false),
                new Field("name", com.mini.paimon.schema.DataType.STRING(), true)
            ),
            Arrays.asList("id"),
            Arrays.asList()
        );
        catalog.createTable(testTable, schema, false);
    }
    
    @After
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
        
        // 清理测试目录
        if (warehouse != null) {
            deleteDirectory(Paths.get(warehouse));
        }
    }
    
    @Test
    public void testCreateEmptyBranch() throws Exception {
        // 创建空分支
        catalog.createBranch(testTable, "dev", null);
        
        // 验证分支存在
        List<String> branches = catalog.listBranches(testTable);
        assertTrue("分支列表应包含 dev", branches.contains("dev"));
        
        // 验证分支目录存在
        Path branchPath = Paths.get(warehouse, "test_db", "test_table", "branch", "branch-dev");
        assertTrue("分支目录应该存在", Files.exists(branchPath));
        
        // 验证分支包含 schema 文件
        Path schemaPath = branchPath.resolve("schema/schema-0");
        assertTrue("分支应该包含 schema 文件", Files.exists(schemaPath));
    }
    
    @Test
    public void testDropBranch() throws Exception {
        // 创建分支
        catalog.createBranch(testTable, "dev", null);
        
        // 验证分支存在
        List<String> branches = catalog.listBranches(testTable);
        assertTrue("分支列表应包含 dev", branches.contains("dev"));
        
        // 删除分支
        catalog.dropBranch(testTable, "dev");
        
        // 验证分支已删除
        branches = catalog.listBranches(testTable);
        assertFalse("分支列表不应包含 dev", branches.contains("dev"));
        
        // 验证分支目录已删除
        Path branchPath = Paths.get(warehouse, "test_db", "test_table", "branch", "branch-dev");
        assertFalse("分支目录应该被删除", Files.exists(branchPath));
    }
    
    @Test
    public void testListBranches() throws Exception {
        // 创建多个分支
        catalog.createBranch(testTable, "dev", null);
        catalog.createBranch(testTable, "test", null);
        catalog.createBranch(testTable, "prod", null);
        
        // 列出所有分支
        List<String> branches = catalog.listBranches(testTable);
        
        // 验证所有分支都存在
        assertEquals("应该有 3 个分支", 3, branches.size());
        assertTrue("应该包含 dev", branches.contains("dev"));
        assertTrue("应该包含 test", branches.contains("test"));
        assertTrue("应该包含 prod", branches.contains("prod"));
    }
    
    @Test
    public void testBranchPathCalculation() {
        // 测试主分支路径
        String mainPath = BranchManager.branchPath("/warehouse/db/table", "main");
        assertEquals("/warehouse/db/table", mainPath);
        
        // 测试自定义分支路径
        String devPath = BranchManager.branchPath("/warehouse/db/table", "dev");
        assertEquals("/warehouse/db/table/branch/branch-dev", devPath);
    }
    
    @Test
    public void testBranchValidation() {
        // 测试合法的分支名
        try {
            BranchManager.validateBranch("dev");
            BranchManager.validateBranch("test_branch");
            BranchManager.validateBranch("branch-1");
        } catch (Exception e) {
            fail("合法的分支名不应该抛出异常: " + e.getMessage());
        }
        
        // 测试非法的分支名 - main
        try {
            BranchManager.validateBranch("main");
            fail("应该拒绝 'main' 作为分支名");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("default branch"));
        }
        
        // 测试非法的分支名 - 纯数字
        try {
            BranchManager.validateBranch("123");
            fail("应该拒绝纯数字作为分支名");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("numeric"));
        }
        
        // 测试非法的分支名 - 空字符串
        try {
            BranchManager.validateBranch("");
            fail("应该拒绝空字符串作为分支名");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("blank"));
        }
    }
    
    @Test
    public void testSwitchToBranch() throws Exception {
        // 创建分支
        catalog.createBranch(testTable, "dev", null);
        
        // 获取主分支的表
        com.mini.paimon.table.Table mainTable = catalog.getTable(testTable);
        assertNotNull("主分支表应该存在", mainTable);
        
        // 切换到 dev 分支
        com.mini.paimon.table.Table devTable = mainTable.switchToBranch("dev");
        assertNotNull("dev 分支表应该存在", devTable);
        
        // 验证是不同的表实例
        assertNotSame("应该返回不同的表实例", mainTable, devTable);
        
        // 切换到同一个分支应该返回相同实例
        com.mini.paimon.table.Table devTable2 = devTable.switchToBranch("dev");
        assertSame("切换到同一分支应该返回相同实例", devTable, devTable2);
    }
    
    /**
     * 辅助方法：递归删除目录
     */
    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            try (Stream<Path> walk = Files.walk(directory)) {
                walk.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            }
        }
    }
}

