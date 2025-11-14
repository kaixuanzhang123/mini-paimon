# Catalog Factory SPI 机制改造总结

## 概述

参考 Apache Paimon 的 CatalogFactory 设计，通过 SPI (Service Provider Interface) 机制实现了 Catalog 的动态加载和配置化创建。这使得系统更加灵活，支持通过配置参数创建不同类型的 Catalog，而不是硬编码实例化。

## 改造内容

### 1. 核心接口和类

#### 1.1 CatalogFactory 接口
- **位置**: `paimon/src/main/java/com/mini/paimon/catalog/CatalogFactory.java`
- **功能**: 定义 Catalog 工厂的统一接口
- **方法**:
  - `String identifier()`: 返回 Catalog 类型的唯一标识符
  - `Catalog create(CatalogContext context)`: 根据上下文创建 Catalog 实例

#### 1.2 FileSystemCatalogFactory 实现
- **位置**: `paimon/src/main/java/com/mini/paimon/catalog/FileSystemCatalogFactory.java`
- **标识符**: `filesystem`
- **支持的配置项**:
  - `catalog.name`: Catalog 名称（默认: "paimon"）
  - `catalog.default-database`: 默认数据库（默认: "default"）
  - `warehouse`: 仓库路径（必需）

#### 1.3 CatalogLoader 工具类
- **位置**: `paimon/src/main/java/com/mini/paimon/catalog/CatalogLoader.java`
- **功能**: 通过 SPI 机制加载和管理 CatalogFactory
- **主要方法**:
  - `load(String identifier, CatalogContext context)`: 根据标识符加载 Catalog
  - `getAvailableIdentifiers()`: 获取所有已注册的标识符
  - `isFactoryRegistered(String identifier)`: 检查标识符是否已注册

### 2. SPI 配置

#### 2.1 服务配置文件
- **位置**: `paimon/src/main/resources/META-INF/services/com.mini.paimon.catalog.CatalogFactory`
- **内容**: 
  ```
  com.mini.paimon.catalog.FileSystemCatalogFactory
  ```

### 3. 模块更新

#### 3.1 Flink 模块
更新的文件：
- `FlinkEnvironmentFactory.java`: 使用 CatalogLoader 创建 Catalog
- `PaimonSource.java`: 使用 CatalogLoader 创建 Catalog
- `PaimonSinkFunction.java`: 使用 CatalogLoader 创建 Catalog

**使用示例**:
```java
CatalogContext catalogContext = CatalogContext.builder()
    .warehouse(warehousePath)
    .option("catalog.name", "paimon")
    .option("catalog.default-database", defaultDatabase)
    .build();
Catalog paimonCatalog = CatalogLoader.load("filesystem", catalogContext);
```

#### 3.2 Spark 模块
更新的文件：
- `SparkCatalog.java`: 使用 CatalogLoader 创建 Catalog
- `PaimonPartitionReaderFactory.java`: 使用 CatalogLoader 创建 Catalog
- `PaimonDataWriterFactory.java`: 使用 CatalogLoader 创建 Catalog
- `PaimonBatchWrite.java`: 使用 CatalogLoader 创建 Catalog

#### 3.3 测试代码
更新的文件：
- `CatalogHelper.java`: 使用 CatalogLoader 创建 Catalog
- 新增 `CatalogFactoryTest.java`: 测试 SPI 机制和 CatalogFactory

## 使用方式对比

### 改造前（硬编码）
```java
CatalogContext context = CatalogContext.builder()
    .warehouse(warehousePath)
    .build();
Catalog catalog = new FileSystemCatalog("paimon", "default", context);
```

### 改造后（SPI + 配置）
```java
CatalogContext context = CatalogContext.builder()
    .warehouse(warehousePath)
    .option("catalog.name", "paimon")
    .option("catalog.default-database", "default")
    .build();
Catalog catalog = CatalogLoader.load("filesystem", context);
```

## 优势

1. **解耦**: Catalog 的创建与具体实现解耦，便于扩展
2. **配置化**: 通过配置参数控制 Catalog 行为，无需修改代码
3. **可扩展**: 可以轻松添加新的 Catalog 实现（如 HiveCatalog）
4. **统一管理**: 所有 Catalog 实现通过 SPI 统一注册和管理
5. **类型安全**: 编译时检查 Catalog 标识符的有效性

## 扩展指南

如果需要添加新的 Catalog 实现（例如 HiveCatalog）：

1. 创建 Factory 实现:
```java
public class HiveCatalogFactory implements CatalogFactory {
    public static final String IDENTIFIER = "hive";
    
    @Override
    public String identifier() {
        return IDENTIFIER;
    }
    
    @Override
    public Catalog create(CatalogContext context) {
        // 从 context 获取配置并创建 HiveCatalog
        return new HiveCatalog(context);
    }
}
```

2. 在 SPI 配置文件中注册:
```
com.mini.paimon.catalog.FileSystemCatalogFactory
com.mini.paimon.catalog.HiveCatalogFactory
```

3. 使用新的 Catalog:
```java
Catalog catalog = CatalogLoader.load("hive", context);
```

## 测试验证

所有测试通过，包括：
- ✅ CatalogFactory 标识符测试
- ✅ SPI 机制加载测试
- ✅ 数据库操作测试
- ✅ 表操作测试
- ✅ 配置选项传递测试
- ✅ 无效标识符异常测试
- ✅ Flink 集成测试
- ✅ Spark 集成测试

## 参考

本实现参考了 Apache Paimon 的以下设计：
- `org.apache.paimon.catalog.CatalogFactory`
- `org.apache.paimon.hive.HiveCatalogFactory`
- `org.apache.paimon.catalog.CatalogContext`

## 总结

通过引入 SPI 机制和 CatalogFactory 设计模式，Mini Paimon 的 Catalog 创建方式更加灵活和可扩展，与 Apache Paimon 的设计理念保持一致，为未来支持更多 Catalog 类型（如 Hive、JDBC 等）奠定了基础。

