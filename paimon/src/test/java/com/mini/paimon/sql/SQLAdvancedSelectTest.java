package com.mini.paimon.sql;

import com.mini.paimon.catalog.*;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.table.*;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SQL 高级查询测试
 * 测试字段投影和 WHERE 条件过滤
 */
public class SQLAdvancedSelectTest {

    public static void main(String[] args) {
        try {
            // 创建路径工厂
            PathFactory pathFactory = new PathFactory("./warehouse");

            // 创建 Catalog
            CatalogContext context = CatalogContext.builder().warehouse("./warehouse").build();
            Catalog catalog = new FileSystemCatalog("default", context);

            // 创建 SQL 解析器
            SQLParser sqlParser = new SQLParser(catalog, pathFactory);

            // 先创建表
            catalog.createDatabase("default", true);

            Identifier identifier = new Identifier("default", "users");
            // 创建Schema
            List<Field> fields = new ArrayList<>();
            fields.add(new Field("id", DataType.INT(), false));
            fields.add(new Field("name", DataType.STRING(), true));
            fields.add(new Field("age", DataType.INT(), true));

            List<String> primaryKeys = Arrays.asList("id");

            Schema schema = new Schema(0, fields, primaryKeys);
            catalog.createTable(identifier, schema, true);

            // 插入数据
            System.out.println("\n2. 插入数据:");
            insertDataWithoutClosing(pathFactory, catalog);

            // 测试 1: SELECT * 查询（全字段）
            System.out.println("\n3. SELECT * FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users");

            // 测试 2: 字段投影查询
            System.out.println("\n4. SELECT name, age FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name, age FROM users");

            // 测试 3: 单字段查询
            System.out.println("\n5. SELECT * FROM users WHERE id = 1:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE id = 1");

            System.out.println("SELECT * FROM users WHERE id > 1 and id < 3:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM default.users WHERE id > 1 and id < 3");

            // 测试 3: 单字段查询
            System.out.println("\n5. SELECT name FROM users:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name FROM users ");

            // 测试 4: WHERE 条件过滤（等于）
            System.out.println("\n6. SELECT * FROM users WHERE age = 30:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age = 30");

            // 测试 5: WHERE 条件过滤（大于）
            System.out.println("\n7. SELECT * FROM users WHERE age > 25:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age > 25");

            // 测试 6: WHERE 条件过滤（小于等于）
            System.out.println("\n8. SELECT * FROM users WHERE age <= 30:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE age <= 30");

            // 测试 7: 字段投影 + WHERE 过滤
            System.out.println("\n9. SELECT name, age FROM users WHERE age > 25:");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT name, age FROM users WHERE age > 25");

            // 测试 8: 字符串条件过滤
            System.out.println("\n10. SELECT * FROM users WHERE name = 'Bob':");
            System.out.println("==================================================");
            sqlParser.executeSQL("SELECT * FROM users WHERE name = 'Bob'");

        } catch (IOException e) {
            System.err.println("执行 SQL 时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 插入数据但不立即关闭LSMTree，以避免多次刷写
     */
    private static void insertDataWithoutClosing(PathFactory pathFactory, Catalog catalog) throws IOException {
        Table table = catalog.getTable(new Identifier("default", "users"));

        TableWrite writer = table.newWrite();
        writer.write(new Row(new Object[]{1, "Alice", 25}));
        System.out.println("插入数据: (1, 'Alice', 25)");

        writer.write(new Row(new Object[]{2, "Bob", 30}));
        System.out.println("插入数据: (2, 'Bob', 30)");

        writer.write(new Row(new Object[]{3, "Charlie", 35}));
        System.out.println("插入数据: (3, 'Charlie', 35)");

        writer.write(new Row(new Object[]{4, "David", 28}));
        System.out.println("插入数据: (4, 'David', 28)");

        writer.write(new Row(new Object[]{5, "Eve", 22}));
        System.out.println("插入数据: (5, 'Eve', 22)");

        writer.write(new Row(new Object[]{5, "Eve1", 23}));
        System.out.println("插入数据: (5, 'Eve1', 23)");

        TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
        TableCommit commit = table.newCommit();
        commit.commit(commitMsg, com.mini.paimon.snapshot.Snapshot.CommitKind.APPEND);
        writer.close();
    }

}