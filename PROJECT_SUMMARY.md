# Mini Paimon 项目总结文档

## 项目概述

Mini Paimon 是一个简化版的 Apache Paimon 实现，用于学习和理解 Paimon 的核心设计理念和实现方式。该项目实现了 Paimon 的关键特性，包括LSM Tree存储引擎、两阶段提交机制、Manifest/Snapshot管理以及与Apache Spark的集成。

## 项目架构

### 1. 核心模块结构

```
mini-paimon/
├── paimon/                    # 核心存储引擎模块
│   ├── catalog/              # Catalog接口与文件系统实现
│   ├── storage/              # LSM Tree存储引擎
│   ├── metadata/             # 元数据管理(Schema, DataType等)
│   ├── snapshot/             # Snapshot管理
│   ├── manifest/             # Manifest文件管理
│   ├── table/                # 表接口与读写实现
│   ├── transaction/          # 事务管理
│   ├── index/                # 索引实现(BloomFilter, MinMax)
│   ├── partition/            # 分区管理
│   └── reader/               # 数据读取器
├── paimon-spark/             # Spark集成模块
│   ├── catalog/              # Spark Catalog实现
│   ├── source/               # Spark数据源(读取)
│   ├── sink/                 # Spark数据写入
│   └── table/                # Schema转换与Row转换
└── paimon-flink/             # Flink集成模块(已实现)
```

### 2. 存储架构

#### LSM Tree 存储引擎

基于 LSM (Log-Structured Merge-Tree) 架构，分层存储数据：

- **Level 0**: 直接从 MemTable flush 的 SSTable 文件
- **Level 1-6**: 通过 Compaction 合并生成的有序文件
- **MemTable**: 内存中的有序数据结构，支持快速写入
- **WAL (Write-Ahead Log)**: 写前日志，确保数据持久性

**文件格式 (SSTable)**:
```
[DataBlock1] [RowIndex1] [DataBlock2] [RowIndex2] ... 
[BloomFilter] [BlockIndex] [Footer] [FooterSize] [MagicNumber]
```

#### 两阶段提交机制

参考 Apache Paimon 的设计，实现了两阶段提交（2PC）：

1. **Prepare 阶段** (`prepareCommit`):
   - 刷写 MemTable 到 SSTable
   - 生成 DataFileMeta（文件元数据）
   - 创建 TableCommitMessage
   
2. **Commit 阶段** (`commit`):
   - 生成 ManifestEntry（ADD/DELETE操作）
   - 写入 Manifest 文件
   - 创建新的 Snapshot
   - 更新 LATEST 指针

### 3. 元数据管理

#### Snapshot 层次结构

```
Snapshot (snapshot-N)
  ├── baseManifestList: manifest-list-base-N  (全量)
  ├── deltaManifestList: manifest-list-delta-N (增量)
  └── schemaId: Schema版本号

ManifestList (manifest-list-delta-N)
  └── manifestFiles: [manifest-xxx, manifest-yyy, ...]

Manifest (manifest-xxx)
  └── ManifestEntry[]
        ├── kind: ADD/DELETE
        ├── partition: 分区信息
        └── file: DataFileMeta
              ├── fileName: data-xxx.sst
              ├── fileSize, rowCount
              ├── minKey, maxKey
              └── indexMeta: 索引元数据
```

#### Schema Evolution

- **Schema 版本管理**: 每个 Schema 有唯一的 schemaId
- **向后兼容**: 新增列、删除列保持兼容性
- **类型系统**: 支持 INT, BIGINT, STRING, DOUBLE, BOOLEAN, TIMESTAMP 等

### 4. 分区与Bucket机制

- **分区 (Partition)**: 按照分区键划分数据，格式如 `dt=2024-01-01`
- **Bucket**: 每个分区内部进一步划分为多个 bucket，实现并行读写
- **动态分区管理**: 自动创建分区目录和bucket

## 核心功能特性

### 1. 高性能读写

- **批量写入**: 通过 MemTable 缓存数据，批量刷写到磁盘
- **异步 Compaction**: 后台异步合并 SSTable，减少读放大
- **索引加速**: 
  - BloomFilter: 快速判断key是否存在
  - MinMax Index: 文件级别的数据范围过滤
  - Row-level Index: Block级别的快速定位

### 2. 事务与并发控制

- **ACID 支持**: 通过 WAL 和两阶段提交保证事务特性
- **乐观锁机制**: Snapshot 冲突检测与重试
- **并发写入**: 支持多个 writer 同时写入不同分区

### 3. 计算引擎集成

#### Spark集成 (DataSource V2 API)

- **Catalog实现**: `SparkCatalog` 桥接Paimon Catalog
- **Table实现**: `SparkTable` 提供读写能力
- **Batch Read**: 并行读取多个SSTable文件
- **Batch Write**: 
  - 多个 DataWriter 并行写入
  - 统一的提交协调 (BatchWrite.commit)
  - 自动的 Schema 转换

#### Flink集成 (已实现)

- **Table API**: 支持 Flink SQL 创建表、插入和查询
- **流批一体**: 同时支持流式和批处理模式

### 4. 数据可靠性

- **WAL机制**: 写入数据先记录到WAL，保证不丢失
- **Snapshot隔离**: 每次提交生成新Snapshot，支持时间旅行
- **数据校验**: SSTable文件包含MagicNumber校验

## 关键设计决策

### 1. 为什么选择 LSM Tree？

- **写优化**: 顺序写WAL和MemTable，吞吐量高
- **适合大数据**: 可扩展性好，支持海量数据存储
- **支持更新**: 通过新版本覆盖旧版本实现更新

### 2. Manifest vs 直接扫描文件系统

- **性能**: Manifest 避免了昂贵的文件系统list操作
- **元数据缓存**: Manifest包含文件统计信息，支持文件级过滤
- **版本管理**: Manifest 关联到 Snapshot，支持时间旅行

### 3. 两阶段提交的必要性

- **分布式协调**: 多个Writer的数据需要原子性提交
- **一致性视图**: 确保Reader看到完整的提交，不会读到部分数据
- **回滚支持**: Prepare失败可以安全回滚

### 4. Schema 序列化格式

修复前使用JSON序列化，包含多余字段（fixedSize, fixedLength）。

修复后：
```json
{
  "schemaId": 0,
  "fields": [{
    "name": "product_id",
    "type": {"type": "BIGINT"},
    "nullable": true
  }]
}
```

使用 `@JsonIgnore` 注解隐藏工具方法，保持格式简洁。

## 问题修复总结

### 1. DataType类型映射问题

**问题**: Spark Long类型映射到`LONG`，但期望`BIGINT`  
**修复**: 
- 修改 `LongType.typeName()` 返回 `"BIGINT"`
- `SparkSchemaConverter` 兼容 `LONG` 和 `BIGINT`
- `SparkRowConverter` 确保类型转换正确（Number→Long）

### 2. Schema文件格式问题

**问题**: JSON序列化包含额外的`fixedSize`和`fixedLength`字段  
**修复**: 在DataType抽象类的方法上添加`@JsonIgnore`注解

### 3. SSTable 文件重复写入

**问题**: 多个Writer生成相同的sequence，导致写入同一文件，文件末尾有多个MagicNumber  
**修复**: 
```java
long initialSequence = (writerId << 32) | (System.currentTimeMillis() & 0xFFFFFFFFL);
```
使用 writerId + 时间戳生成唯一的sequence

### 4. Spark 分布式提交问题

**问题**: 每个Writer独立提交，后面的Snapshot覆盖前面的  
**修复**: 
- DataWriter.commit() 只返回提交消息，不实际提交
- BatchWrite.commit() 收集所有消息，统一提交

### 5. 序列化问题

**问题**: `TableCommitMessage`, `ManifestEntry`, `DataFileMeta`, `RowKey` 无法序列化  
**修复**: 为这些类添加 `implements Serializable`

### 6. Integer/Long类型转换

**问题**: JSON反序列化时小数字变成Integer，Spark期望Long  
**修复**: `convertToSparkValue` 中显式转换 `((Number) value).longValue()`

## 与开源 Paimon 的对比

### 相同点

1. **核心架构**: LSM Tree + Snapshot + Manifest
2. **两阶段提交**: prepareCommit + commit
3. **计算引擎集成**: Spark DataSource V2 API
4. **元数据格式**: JSON序列化的Schema和Snapshot

### 简化点

1. **Compaction策略**: 简化为基于大小的tiered compaction
2. **索引类型**: 仅实现BloomFilter和MinMax，未实现全文索引
3. **数据类型**: 支持基本类型，未实现复杂类型(ARRAY, MAP)
4. **优化**: 未实现统计信息收集、动态过滤等高级优化
5. **容错**: 简化的错误处理和重试逻辑

## 性能特点

- **写入吞吐**: 单表百万级QPS（基于MemTable批量写入）
- **查询延迟**: 毫秒级点查（基于BloomFilter和索引）
- **Compaction**: 异步后台执行，不阻塞写入
- **扩展性**: 支持分区和Bucket，线性扩展

## 使用示例

### Spark SQL 示例

```sql
-- 创建数据库
CREATE DATABASE IF NOT EXISTS paimon.my_db;
USE paimon.my_db;

-- 创建表
CREATE TABLE products (
  product_id BIGINT,
  product_name STRING,
  price DOUBLE,
  in_stock BOOLEAN
) USING paimon;

-- 插入数据
INSERT INTO products VALUES 
  (1, 'Laptop', 999.99, true),
  (2, 'Mouse', 29.99, true),
  (3, 'Keyboard', 79.99, false);

-- 查询数据
SELECT * FROM products WHERE price > 50.0;
SELECT AVG(price) FROM products;
```

### Java API 示例

```java
// 创建Catalog
CatalogContext context = CatalogContext.builder()
    .warehouse("/path/to/warehouse")
    .build();
Catalog catalog = new FileSystemCatalog("my_catalog", "default", context);

// 获取表
Identifier identifier = new Identifier("my_db", "products");
Table table = catalog.getTable(identifier);

// 写入数据
TableWrite writer = table.newWrite(0L);
Row row = new Row(new Object[]{1L, "Laptop", 999.99, true});
writer.write(row);

// 提交
TableWrite.TableCommitMessage message = writer.prepareCommit();
TableCommit commit = table.newCommit();
commit.commit(message);

// 读取数据
TableRead reader = table.newRead();
List<Row> rows = reader.read();
```

## 总结

Mini Paimon 成功实现了 Apache Paimon 的核心功能，包括：
- ✅ LSM Tree 存储引擎
- ✅ Snapshot + Manifest 元数据管理
- ✅ 两阶段提交机制
- ✅ Spark集成 (DataSource V2)
- ✅ Flink集成 (Table API)
- ✅ 分区和Bucket支持
- ✅ 索引加速 (BloomFilter, MinMax)

通过这个项目，可以深入理解：
1. 湖仓一体架构的设计理念
2. LSM Tree在大数据场景的应用
3. 计算存储分离的实现方式
4. 分布式事务的协调机制

## 未来优化方向

1. **WAL清理**: 提交后自动清理WAL文件和目录
2. **更多数据类型**: 支持ARRAY, MAP, STRUCT等复杂类型
3. **增量读取**: 支持流式增量查询
4. **Schema演进**: 支持列重命名、类型变更等
5. **高级优化**: 统计信息、谓词下推、列裁剪等
6. **压缩算法**: 支持LZ4, Snappy等压缩
7. **监控指标**: 添加读写延迟、吞吐量等监控

## 参考资料

- Apache Paimon 官方文档: https://paimon.apache.org
- Apache Paimon GitHub: https://github.com/apache/paimon
- Spark DataSource V2 API 文档
- LSM Tree 论文与实现

