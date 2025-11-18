# 分支表 Procedure 使用指南

本文档说明如何使用 Flink 和 Spark 的 Procedure 来管理分支表。

## 概述

已实现的分支管理 Procedures：
- **create_branch**: 创建分支
- **drop_branch**: 删除分支  
- **list_branches**: 列出所有分支

## Flink Procedure

### 实现说明

由于 Flink 1.17.x 的标准 Procedure 接口尚未完全稳定，我们实现了简化版本的 Procedure。

**实现的类：**
- `ProcedureBase`: Procedure 基类
- `CreateBranchProcedure`: 创建分支
- `DropBranchProcedure`: 删除分支
- `ListBranchesProcedure`: 列出分支

### 使用方式

通过 Java API 调用：

```java
import com.mini.paimon.flink.catalog.FlinkCatalog;
import com.mini.paimon.flink.procedure.*;

// 获取 FlinkCatalog 实例
FlinkCatalog flinkCatalog = ...;

// 1. 创建分支
ProcedureBase createProc = flinkCatalog.getProcedure("create_branch");
if (createProc instanceof CreateBranchProcedure) {
    CreateBranchProcedure proc = (CreateBranchProcedure) createProc;
    
    // 创建空分支
    String result = proc.call("database.table", "branch_name");
    System.out.println(result);
    
    // 从 Tag 创建分支
    result = proc.call("database.table", "branch_name", "tag_name");
    System.out.println(result);
}

// 2. 删除分支
ProcedureBase dropProc = flinkCatalog.getProcedure("drop_branch");
if (dropProc instanceof DropBranchProcedure) {
    DropBranchProcedure proc = (DropBranchProcedure) dropProc;
    String result = proc.call("database.table", "branch_name");
    System.out.println(result);
}

// 3. 列出分支
ProcedureBase listProc = flinkCatalog.getProcedure("list_branches");
if (listProc instanceof ListBranchesProcedure) {
    ListBranchesProcedure proc = (ListBranchesProcedure) listProc;
    List<String> branches = proc.call("database.table");
    branches.forEach(System.out::println);
}

// 列出所有可用的 Procedure
List<String> procedures = flinkCatalog.listProcedures();
System.out.println("Available procedures: " + procedures);
```

### 访问分支表

通过表名语法访问分支：

```java
// Flink Table API
tableEnv.executeSql("SELECT * FROM `my_table$branch_dev`");

// Flink SQL
tableEnv.executeSql("INSERT INTO `my_table$branch_dev` VALUES (1, 'test')");
```

## Spark Procedure

### 实现说明

由于 Spark 3.3.x 没有标准的 Procedure 接口，我们实现了自定义的 Procedure 机制。

**实现的类：**
- `BaseProcedure`: Procedure 基类
- `CreateBranchProcedure`: 创建分支
- `DropBranchProcedure`: 删除分支
- `ListBranchesProcedure`: 列出分支

### 使用方式

通过 Java/Scala API 调用：

#### Java 示例

```java
import com.mini.paimon.spark.catalog.SparkCatalog;
import com.mini.paimon.spark.procedure.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;

// 获取 SparkCatalog 实例
SparkCatalog sparkCatalog = ...;

// 1. 创建分支
BaseProcedure createProc = sparkCatalog.getProcedure("create_branch");
if (createProc instanceof CreateBranchProcedure) {
    CreateBranchProcedure proc = (CreateBranchProcedure) createProc;
    
    // 创建空分支
    InternalRow args = InternalRow.apply(
        UTF8String.fromString("database.table"),
        UTF8String.fromString("branch_name"),
        null
    );
    InternalRow[] result = proc.call(args);
    // 处理结果...
}

// 2. 删除分支
BaseProcedure dropProc = sparkCatalog.getProcedure("drop_branch");
if (dropProc instanceof DropBranchProcedure) {
    DropBranchProcedure proc = (DropBranchProcedure) dropProc;
    
    InternalRow args = InternalRow.apply(
        UTF8String.fromString("database.table"),
        UTF8String.fromString("branch_name")
    );
    InternalRow[] result = proc.call(args);
    // 处理结果...
}

// 3. 列出分支
BaseProcedure listProc = sparkCatalog.getProcedure("list_branches");
if (listProc instanceof ListBranchesProcedure) {
    ListBranchesProcedure proc = (ListBranchesProcedure) listProc;
    
    InternalRow args = InternalRow.apply(
        UTF8String.fromString("database.table")
    );
    InternalRow[] result = proc.call(args);
    // 结果是分支列表，每个分支一行
}

// 列出所有可用的 Procedure
List<String> procedures = sparkCatalog.listProcedures();
System.out.println("Available procedures: " + procedures);
```

#### Scala 示例

```scala
import com.mini.paimon.spark.catalog.SparkCatalog
import com.mini.paimon.spark.procedure._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

// 获取 SparkCatalog 实例
val sparkCatalog: SparkCatalog = ...

// 1. 创建分支
sparkCatalog.getProcedure("create_branch") match {
  case proc: CreateBranchProcedure =>
    val args = InternalRow(
      UTF8String.fromString("database.table"),
      UTF8String.fromString("branch_name"),
      null
    )
    val result = proc.call(args)
    // 处理结果...
  case _ => // 处理 null 情况
}

// 2. 删除分支
sparkCatalog.getProcedure("drop_branch") match {
  case proc: DropBranchProcedure =>
    val args = InternalRow(
      UTF8String.fromString("database.table"),
      UTF8String.fromString("branch_name")
    )
    val result = proc.call(args)
    // 处理结果...
  case _ => // 处理 null 情况
}

// 3. 列出分支
sparkCatalog.getProcedure("list_branches") match {
  case proc: ListBranchesProcedure =>
    val args = InternalRow(
      UTF8String.fromString("database.table")
    )
    val result = proc.call(args)
    result.foreach(row => println(row.getUTF8String(0)))
  case _ => // 处理 null 情况
}
```

### 访问分支表

通过表名语法访问分支：

```scala
// Spark SQL
spark.sql("SELECT * FROM `my_table$branch_dev`")

// DataFrame API
spark.table("my_table$branch_dev")

// 写入分支表
spark.sql("INSERT INTO `my_table$branch_dev` VALUES (1, 'test')")
```

## 完整示例

### Flink 完整示例

```java
import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.CatalogContext;
import com.mini.paimon.catalog.CatalogLoader;
import com.mini.paimon.flink.catalog.FlinkCatalog;
import com.mini.paimon.flink.procedure.*;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkBranchExample {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink Table Environment
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inBatchMode()
            .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        
        // 2. 创建 Paimon Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse("/path/to/warehouse")
            .build();
        Catalog catalog = CatalogLoader.load("filesystem", context);
        FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", catalog, "default", "/path/to/warehouse");
        
        // 3. 注册 Catalog
        tableEnv.registerCatalog("paimon", flinkCatalog);
        tableEnv.useCatalog("paimon");
        
        // 4. 创建表
        tableEnv.executeSql(
            "CREATE TABLE test_db.test_table (" +
            "  id INT," +
            "  name STRING," +
            "  PRIMARY KEY (id) NOT ENFORCED" +
            ")"
        );
        
        // 5. 插入数据到主分支
        tableEnv.executeSql("INSERT INTO test_db.test_table VALUES (1, 'Alice')");
        
        // 6. 使用 Procedure 创建分支
        CreateBranchProcedure createProc = 
            (CreateBranchProcedure) flinkCatalog.getProcedure("create_branch");
        String result = createProc.call("test_db.test_table", "dev");
        System.out.println(result);
        
        // 7. 向分支写入数据
        tableEnv.executeSql("INSERT INTO `test_db.test_table$branch_dev` VALUES (2, 'Bob')");
        
        // 8. 列出分支
        ListBranchesProcedure listProc = 
            (ListBranchesProcedure) flinkCatalog.getProcedure("list_branches");
        var branches = listProc.call("test_db.test_table");
        System.out.println("Branches: " + branches);
        
        // 9. 查询分支数据
        tableEnv.executeSql("SELECT * FROM `test_db.test_table$branch_dev`").print();
        
        // 10. 删除分支
        DropBranchProcedure dropProc = 
            (DropBranchProcedure) flinkCatalog.getProcedure("drop_branch");
        result = dropProc.call("test_db.test_table", "dev");
        System.out.println(result);
    }
}
```

### Spark 完整示例

```scala
import com.mini.paimon.catalog.{Catalog, CatalogContext, CatalogLoader}
import com.mini.paimon.spark.catalog.SparkCatalog
import com.mini.paimon.spark.procedure._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

object SparkBranchExample {
  def main(args: Array[String]): Unit = {
    // 1. 创建 Spark Session
    val spark = SparkSession.builder()
      .appName("Paimon Branch Example")
      .master("local[*]")
      .config("spark.sql.catalog.paimon", "com.mini.paimon.spark.catalog.SparkCatalog")
      .config("spark.sql.catalog.paimon.warehouse", "/path/to/warehouse")
      .getOrCreate()
    
    // 2. 获取 SparkCatalog 实例
    val sparkCatalog = spark.sessionState.catalog.catalog("paimon").asInstanceOf[SparkCatalog]
    
    // 3. 创建表
    spark.sql(
      """CREATE TABLE paimon.test_db.test_table (
        |  id INT,
        |  name STRING
        |) USING paimon
      """.stripMargin)
    
    // 4. 插入数据到主分支
    spark.sql("INSERT INTO paimon.test_db.test_table VALUES (1, 'Alice')")
    
    // 5. 使用 Procedure 创建分支
    sparkCatalog.getProcedure("create_branch") match {
      case proc: CreateBranchProcedure =>
        val args = InternalRow(
          UTF8String.fromString("test_db.test_table"),
          UTF8String.fromString("dev"),
          null
        )
        val result = proc.call(args)
        println(s"Created branch: ${result(0).getUTF8String(0)}")
      case _ =>
    }
    
    // 6. 向分支写入数据
    spark.sql("INSERT INTO paimon.`test_db.test_table$branch_dev` VALUES (2, 'Bob')")
    
    // 7. 列出分支
    sparkCatalog.getProcedure("list_branches") match {
      case proc: ListBranchesProcedure =>
        val args = InternalRow(UTF8String.fromString("test_db.test_table"))
        val branches = proc.call(args)
        println("Branches:")
        branches.foreach(row => println(s"  - ${row.getUTF8String(0)}"))
      case _ =>
    }
    
    // 8. 查询分支数据
    spark.sql("SELECT * FROM paimon.`test_db.test_table$branch_dev`").show()
    
    // 9. 删除分支
    sparkCatalog.getProcedure("drop_branch") match {
      case proc: DropBranchProcedure =>
        val args = InternalRow(
          UTF8String.fromString("test_db.test_table"),
          UTF8String.fromString("dev")
        )
        val result = proc.call(args)
        println(s"Dropped branch: ${result(0).getUTF8String(0)}")
      case _ =>
    }
    
    spark.stop()
  }
}
```

## 注意事项

1. **Flink 版本**: Flink 1.17.x 的 Procedure 接口尚未完全稳定，本实现使用了简化版本
2. **Spark 版本**: Spark 3.3.x 没有标准的 Procedure 接口，本实现使用了自定义机制
3. **表名语法**: 访问分支表时需要使用反引号包裹表名，例如 `` `table$branch_dev` ``
4. **参数格式**: 表标识符格式为 `database.table`，不包含 Catalog 名称
5. **分支命名**: 分支名不能为纯数字，不能为空，不能是保留名称 `main`

## 架构说明

### 模块结构

```
paimon-flink/
  └── src/main/java/com/mini/paimon/flink/
      ├── catalog/
      │   └── FlinkCatalog.java (扩展了 getProcedure 和 listProcedures 方法)
      └── procedure/
          ├── ProcedureBase.java
          ├── CreateBranchProcedure.java
          ├── DropBranchProcedure.java
          └── ListBranchesProcedure.java

paimon-spark/
  └── src/main/java/com/mini/paimon/spark/
      ├── catalog/
      │   └── SparkCatalog.java (扩展了 getProcedure 和 listProcedures 方法)
      └── procedure/
          ├── BaseProcedure.java
          ├── CreateBranchProcedure.java
          ├── DropBranchProcedure.java
          └── ListBranchesProcedure.java
```

### 核心设计

1. **基类抽象**: `ProcedureBase`/`BaseProcedure` 提供通用的 Catalog 访问和参数验证
2. **独立实现**: 每个 Procedure 功能独立实现，易于维护和扩展
3. **统一接口**: 通过 Catalog 的 `getProcedure` 方法统一获取 Procedure 实例
4. **错误处理**: 完善的参数验证和异常处理，提供清晰的错误信息

## 后续扩展

如需支持标准的 CALL 语法（如 `CALL sys.create_branch('db.table', 'branch')`），需要：

1. **Flink**: 升级到 Flink 1.18+ 并实现标准 Procedure 接口
2. **Spark**: 升级到 Spark 3.5+ 并实现 `TableCatalog.loadProcedure` 方法
3. **注册机制**: 实现 Procedure 的自动注册和发现机制

