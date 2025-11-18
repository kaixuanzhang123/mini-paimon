package com.mini.paimon.branch;

import com.mini.paimon.catalog.*;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableWrite;
import com.mini.paimon.schema.Row;

import java.util.Arrays;
import java.util.List;

/**
 * Mini-Paimon 分支表使用示例
 * 
 * 演示如何使用分支功能进行数据隔离和实验
 */
public class BranchExample {
    
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Catalog
        String warehouse = "./tmp/mini-paimon-branch-demo";
        CatalogContext context = CatalogContext.builder()
            .warehouse(warehouse)
            .option("catalog.name", "demo")
            .option("catalog.default-database", "default")
            .build();
        
        Catalog catalog = CatalogLoader.load("filesystem", context);
        
        try {
            // 2. 创建数据库和表
            catalog.createDatabase("demo_db", true);
            
            Identifier tableId = new Identifier("demo_db", "users");
            Schema schema = new Schema(
                0,
                Arrays.asList(
                    new Field("id", DataType.INT(), false),
                    new Field("name", DataType.STRING(), true),
                    new Field("age", DataType.INT(), true)
                ),
                Arrays.asList("id"),
                Arrays.asList()
            );
            
            catalog.createTable(tableId, schema, true);
            System.out.println("✓ 创建表: demo_db.users");
            
            // 3. 向主分支写入数据
            Table mainTable = catalog.getTable(tableId);
            TableWrite writer = mainTable.newWrite();
            TableCommit committer = mainTable.newCommit();
            
            // 创建一行数据
            Row row1 = new Row(new Object[]{1, "Alice", 25});
            writer.write(row1);
            
            committer.commit(writer.prepareCommit());
            writer.close();
            
            System.out.println("✓ 主分支写入数据: id=1, name=Alice, age=25");
            
            // 4. 创建dev分支进行实验
            catalog.createBranch(tableId, "dev", null);
            System.out.println("✓ 创建dev分支");
            
            // 5. 列出所有分支
            List<String> branches = catalog.listBranches(tableId);
            System.out.println("✓ 分支列表: " + branches);
            
            // 6. 切换到dev分支并写入不同的数据
            Table devTable = mainTable.switchToBranch("dev");
            TableWrite devWriter = devTable.newWrite();
            TableCommit devCommitter = devTable.newCommit();
            
            // 创建另一行数据
            Row row2 = new Row(new Object[]{2, "Bob", 30});
            devWriter.write(row2);
            
            devCommitter.commit(devWriter.prepareCommit());
            devWriter.close();
            
            System.out.println("✓ dev分支写入数据: id=2, name=Bob, age=30");
            
            // 7. 通过table$branch_xxx语法访问分支
            Identifier devTableId = new Identifier("demo_db", "users$branch_dev");
            Table devTableByName = catalog.getTable(devTableId);
            System.out.println("✓ 通过 'users$branch_dev' 语法访问dev分支");
            
            // 8. 创建另一个分支进行不同的实验
            catalog.createBranch(tableId, "test", null);
            System.out.println("✓ 创建test分支");
            
            branches = catalog.listBranches(tableId);
            System.out.println("✓ 更新后的分支列表: " + branches);
            
            // 9. 删除test分支
            catalog.dropBranch(tableId, "test");
            System.out.println("✓ 删除test分支");
            
            branches = catalog.listBranches(tableId);
            System.out.println("✓ 最终分支列表: " + branches);
            
            System.out.println("\n=== 分支功能演示完成 ===");
            System.out.println("主分支和dev分支现在包含不同的数据，完全隔离");
            System.out.println("仓库位置: " + warehouse);
            
        } finally {
            catalog.close();
        }
    }
}

