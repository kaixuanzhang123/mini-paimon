# CatalogFactory 使用指南

## 简介

通过 SPI 机制，Mini Paimon 现在支持通过配置创建 Catalog，而不是硬编码实例化。这使得系统更加灵活和可扩展。

## 基本使用

### 1. 创建 Catalog（推荐方式）

```java
// 使用 CatalogLoader 通过 SPI 机制加载 Catalog
CatalogContext context = CatalogContext.builder()
    .warehouse("/path/to/warehouse")
    .option("catalog.name", "my_catalog")
    .option("catalog.default-database", "default")
    .build();

Catalog catalog = CatalogLoader.load("filesystem", context);
```

### 2. 旧方式（不推荐，但仍然支持）

```java
// 直接实例化（不推荐）
CatalogContext context = CatalogContext.builder()
    .warehouse("/path/to/warehouse")
    .build();

Catalog catalog = new FileSystemCatalog("my_catalog", "default", context);
```

## 配置选项

### 必需配置
- `warehouse`: 仓库路径（必需）

### 可选配置
- `catalog.name`: Catalog 名称（默认: "paimon"）
- `catalog.default-database`: 默认数据库（默认: "default"）

## Flink 集成

```java
import com.mini.paimon.catalog.CatalogLoader;
import com.mini.paimon.flink.utils.FlinkEnvironmentFactory;

// 使用工厂方法（内部使用 CatalogLoader）
TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment(
    "./warehouse",
    "default"
);

// 或者手动创建
CatalogContext context = CatalogContext.builder()
    .warehouse("./warehouse")
    .option("catalog.name", "paimon")
    .option("catalog.default-database", "default")
    .build();

Catalog paimonCatalog = CatalogLoader.load("filesystem", context);
FlinkCatalog flinkCatalog = new FlinkCatalog("paimon", paimonCatalog, "default", "./warehouse");

tableEnv.registerCatalog("paimon", flinkCatalog);
tableEnv.useCatalog("paimon");
```

## Spark 集成

```java
import com.mini.paimon.spark.utils.SparkSessionFactory;

// 使用工厂方法（内部使用 CatalogLoader）
SparkSession spark = SparkSessionFactory.createSparkSession("./warehouse");

// Spark Catalog 会自动通过 SPI 加载 FileSystemCatalog
spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
spark.sql("USE test_db");
```

## 完整示例

```java
package com.mini.paimon.example;

import com.mini.paimon.catalog.*;
import com.mini.paimon.metadata.*;
import com.mini.paimon.table.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CatalogExample {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Catalog
        CatalogContext context = CatalogContext.builder()
            .warehouse("./warehouse")
            .option("catalog.name", "example")
            .option("catalog.default-database", "default")
            .build();
        
        Catalog catalog = CatalogLoader.load("filesystem", context);
        
        // 2. 创建数据库
        catalog.createDatabase("test_db", true);
        
        // 3. 创建表
        Identifier identifier = new Identifier("test_db", "users");
        Schema schema = new Schema(
            0,
            Arrays.asList(
                new Field("id", DataType.LONG(), false),
                new Field("name", DataType.STRING(), true),
                new Field("age", DataType.INT(), true)
            ),
            Collections.singletonList("id"),
            Collections.emptyList()
        );
        
        catalog.createTable(identifier, schema, true);
        
        // 4. 写入数据
        Table table = catalog.getTable(identifier);
        TableWrite tableWrite = table.newWrite();
        
        tableWrite.write(new Row(Arrays.asList(1L, "Alice", 25)));
        tableWrite.write(new Row(Arrays.asList(2L, "Bob", 30)));
        
        TableCommit tableCommit = table.newCommit();
        tableCommit.commit(tableWrite.prepareCommit());
        tableWrite.close();
        
        // 5. 读取数据
        TableRead tableRead = table.newRead();
        TableScan.Plan plan = table.newScan().plan();
        List<Row> rows = tableRead.read(plan);
        
        for (Row row : rows) {
            System.out.println(row);
        }
        
        // 6. 关闭 Catalog
        catalog.close();
    }
}
```

## 查看可用的 Catalog 类型

```java
import com.mini.paimon.catalog.CatalogLoader;

// 获取所有已注册的 Catalog 类型
Set<String> identifiers = CatalogLoader.getAvailableIdentifiers();
System.out.println("Available catalog types: " + identifiers);
// 输出: Available catalog types: [filesystem]

// 检查特定类型是否已注册
boolean isRegistered = CatalogLoader.isFactoryRegistered("filesystem");
System.out.println("Is filesystem registered: " + isRegistered);
// 输出: Is filesystem registered: true
```

## 扩展：添加新的 Catalog 类型

如果需要支持新的 Catalog 类型（如 HiveCatalog），按以下步骤操作：

### 1. 创建 Factory 实现

```java
package com.mini.paimon.catalog;

public class HiveCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = "hive";
    
    // Hive 特定配置
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    
    @Override
    public String identifier() {
        return IDENTIFIER;
    }
    
    @Override
    public Catalog create(CatalogContext context) {
        String catalogName = context.getOption("catalog.name", "hive");
        String defaultDatabase = context.getOption("catalog.default-database", "default");
        String metastoreUris = context.getOption(HIVE_METASTORE_URIS);
        
        return new HiveCatalog(catalogName, defaultDatabase, metastoreUris, context);
    }
}
```

### 2. 注册到 SPI

在 `src/main/resources/META-INF/services/com.mini.paimon.catalog.CatalogFactory` 文件中添加：

```
com.mini.paimon.catalog.FileSystemCatalogFactory
com.mini.paimon.catalog.HiveCatalogFactory
```

### 3. 使用新的 Catalog

```java
CatalogContext context = CatalogContext.builder()
    .warehouse("/path/to/warehouse")
    .option("catalog.name", "hive_catalog")
    .option("catalog.default-database", "default")
    .option("hive.metastore.uris", "thrift://localhost:9083")
    .build();

Catalog catalog = CatalogLoader.load("hive", context);
```

## 注意事项

1. **SPI 配置文件**: 确保 `META-INF/services/com.mini.paimon.catalog.CatalogFactory` 文件存在且正确配置
2. **类加载器**: SPI 机制依赖于类加载器，确保 Factory 类在 classpath 中
3. **配置传递**: 所有配置都通过 `CatalogContext` 传递，避免硬编码
4. **向后兼容**: 旧的直接实例化方式仍然支持，但推荐使用 SPI 方式

## 测试

运行 CatalogFactory 测试：

```bash
cd paimon
mvn test -Dtest=CatalogFactoryTest
```

所有测试应该通过，包括：
- ✅ SPI 机制加载测试
- ✅ Catalog 创建测试
- ✅ 数据库操作测试
- ✅ 表操作测试
- ✅ 配置传递测试

## 参考

- [Apache Paimon CatalogFactory](https://github.com/apache/paimon)
- [Java SPI 机制](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html)

