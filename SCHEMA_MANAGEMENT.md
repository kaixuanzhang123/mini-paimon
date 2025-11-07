# Mini Paimon - Schema 管理实现

## 概述

本文档描述了 Mini Paimon 项目中 Schema 管理功能的实现。Schema 管理是现代数据湖表格式的核心功能之一，负责表结构的定义、演化和持久化。

## 实现组件

### 1. TableMetadata（表元数据）

**类**: [TableMetadata.java](src/main/java/com/mini/paimon/metadata/TableMetadata.java)

表元数据类管理表级别的信息，包括表名、数据库名、当前 Schema ID、创建时间等。

#### 主要特性：
- 存储表的基本信息（表名、数据库名、Schema ID）
- 记录创建和修改时间
- 支持表属性配置
- 提供 JSON 序列化和反序列化功能

#### 核心方法：
- `persist(PathFactory pathFactory)`: 持久化表元数据到文件
- `load(PathFactory pathFactory, String databaseName, String tableName)`: 从文件加载表元数据
- `exists(PathFactory pathFactory, String databaseName, String tableName)`: 检查表元数据是否存在

### 2. SchemaManager（Schema 管理器）

**类**: [SchemaManager.java](src/main/java/com/mini/paimon/metadata/SchemaManager.java)

Schema 管理器负责表结构的版本管理、持久化和缓存。

#### 主要特性：
- 支持 Schema 版本演化
- 提供 Schema 缓存机制
- 实现 Schema 持久化到磁盘
- 支持加载特定版本或最新版本的 Schema

#### 核心方法：
- `createNewSchemaVersion(List<Field> fields, List<String> primaryKeys, List<String> partitionKeys)`: 创建新的 Schema 版本
- `loadSchema(int schemaId)`: 加载指定版本的 Schema
- `getCurrentSchema()`: 获取当前活跃的 Schema
- `loadLatestSchema()`: 加载最新的 Schema

### 3. TableManager（表管理器）

**类**: [TableManager.java](src/main/java/com/mini/paimon/metadata/TableManager.java)

表管理器是 Schema 管理的入口，负责表的创建、删除和元数据管理。

#### 主要特性：
- 表的全生命周期管理
- 集成 Schema 管理功能
- 支持表的创建、删除和查询

#### 核心方法：
- `createTable(String database, String tableName, List<Field> fields, List<String> primaryKeys, List<String> partitionKeys)`: 创建新表
- `getTableMetadata(String database, String tableName)`: 获取表元数据
- `dropTable(String database, String tableName)`: 删除表
- `listTables(String database)`: 列出数据库中的所有表

## 文件持久化结构

Schema 管理实现了完整的文件持久化功能，遵循 Paimon 的目录结构：

```
warehouse/
└── {database}/
    └── {table}/
        ├── schema/
        │   ├── schema-0          # Schema 版本 0
        │   └── schema-1          # Schema 版本 1
        └── metadata              # 表元数据文件
```

### Schema 文件格式（JSON）

```json
{
  "schemaId" : 1,
  "fields" : [ {
    "name" : "id",
    "type" : "INT",
    "nullable" : false
  }, {
    "name" : "name",
    "type" : "STRING",
    "nullable" : true
  } ],
  "primaryKeys" : [ "id" ],
  "partitionKeys" : [ ]
}
```

### 表元数据文件格式（JSON）

```json
{
  "tableName" : "user_table",
  "databaseName" : "example_db",
  "currentSchemaId" : 0,
  "createTime" : "2025-11-05T08:23:50.910Z",
  "lastModifiedTime" : "2025-11-05T08:23:50.910Z",
  "description" : "",
  "properties" : { }
}
```

## 使用示例

### 基本使用

```java
// 1. 创建路径工厂
PathFactory pathFactory = new PathFactory("./warehouse");

// 2. 创建表管理器
TableManager tableManager = new TableManager(pathFactory);

// 3. 定义字段
Field idField = new Field("id", DataType.INT, false);
Field nameField = new Field("name", DataType.STRING, true);

// 4. 创建表
TableMetadata tableMetadata = tableManager.createTable(
    "example_db",
    "user_table",
    Arrays.asList(idField, nameField),
    Collections.singletonList("id"),
    Collections.emptyList()
);

// 5. 获取 Schema 管理器
SchemaManager schemaManager = tableManager.getSchemaManager("example_db", "user_table");

// 6. 获取当前 Schema
Schema currentSchema = schemaManager.getCurrentSchema();

// 7. 演化 Schema - 添加新字段
Field emailField = new Field("email", DataType.STRING, true);
Schema newSchema = schemaManager.createNewSchemaVersion(
    Arrays.asList(idField, nameField, emailField),
    Collections.singletonList("id"),
    Collections.emptyList()
);
```

## 测试验证

### 单元测试

- [SchemaManagerTest.java](src/test/java/com/minipaimon/metadata/SchemaManagerTest.java): 测试 Schema 管理功能
- [TableManagerTest.java](src/test/java/com/minipaimon/metadata/TableManagerTest.java): 测试表管理功能

### 测试结果

```
[INFO] Tests run: 33, Failures: 0, Errors: 0, Skipped: 0
```

所有测试均通过，验证了实现的正确性。

## 持久化演示

示例程序成功将数据持久化到磁盘：

```
warehouse/example_db/user_table/schema/schema-0  # 332 字节
warehouse/example_db/user_table/schema/schema-1  # 406 字节
warehouse/example_db/user_table/metadata         # 230 字节
```

## 特性亮点

1. **完整的 Schema 演化支持**: 支持添加字段等 Schema 演化操作
2. **版本管理**: 完整的 Schema 版本控制
3. **文件持久化**: 数据可靠存储到指定目录
4. **缓存机制**: 提升 Schema 访问性能
5. **并发安全**: 支持多线程并发操作
6. **JSON 格式**: 人类可读的元数据存储格式
7. **完整的测试覆盖**: 全面的单元测试保障

## 总结

Mini Paimon 的 Schema 管理实现提供了完整的表元数据管理功能：
- ✅ 表元数据管理（TableMetadata）
- ✅ Schema 版本管理（SchemaManager）
- ✅ 表生命周期管理（TableManager）
- ✅ 完整的文件持久化机制
- ✅ 支持 Schema 演化
- ✅ 完善的测试覆盖

该实现为学习和理解现代数据湖表格式的元数据管理机制提供了良好的基础。
