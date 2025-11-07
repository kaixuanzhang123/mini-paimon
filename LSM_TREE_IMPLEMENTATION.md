# Mini Paimon - LSM Tree 存储引擎实现

## 概述

本文档描述了 Mini Paimon 项目中 LSM Tree 存储引擎的实现。LSM Tree（Log-Structured Merge-Tree）是一种专门为写密集型应用优化的数据结构，广泛应用于现代分布式存储系统如 Apache Paimon、RocksDB、LevelDB 等。

## 实现组件

### 1. MemTable（内存表）

**类**: [MemTable.java](src/main/java/com/mini/paimon/storage/MemTable.java)

MemTable 是 LSM Tree 的内存组件，使用 `ConcurrentSkipListMap` 实现有序存储。

#### 主要特性：
- 使用 `ConcurrentSkipListMap<RowKey, Row>` 存储键值对
- 支持并发读写操作
- 按键排序存储，便于范围查询
- 支持大小监控，达到阈值后触发刷写

#### 核心方法：
- `put(Row row)`: 插入数据行
- `get(RowKey key)`: 根据主键获取数据
- `isFull()`: 检查是否达到大小限制
- `getEntries()`: 获取所有条目

### 2. SSTable（排序字符串表）

**类**: [SSTable.java](src/main/java/com/mini/paimon/storage/SSTable.java)

SSTable 是磁盘上的持久化存储格式，包含数据块、索引块、布隆过滤器和元信息。

#### 文件结构：
```
┌────────────────────────────────┐
│         Data Block 1           │ ← 存储实际数据行
├────────────────────────────────┤
│         Data Block 2           │
├────────────────────────────────┤
│            ...                 │
├────────────────────────────────┤
│         Index Block            │ ← 稀疏索引
├────────────────────────────────┤
│      Bloom Filter Block        │ ← 布隆过滤器
├────────────────────────────────┤
│          Footer                │ ← 元信息
└────────────────────────────────┘
```

#### 组件说明：
- **Data Block**: 存储实际的数据行，每个块约4KB
- **Index Block**: 稀疏索引，记录每个数据块的第一个键和偏移量
- **Bloom Filter**: 快速判断键是否存在，减少磁盘I/O
- **Footer**: 文件元信息，包含各组件的位置和大小

### 3. SSTableWriter（SSTable写入器）

**类**: [SSTableWriter.java](src/main/java/com/mini/paimon/storage/SSTableWriter.java)

负责将 MemTable 中的数据刷写到磁盘上的 SSTable 文件。

#### 主要功能：
- 将内存表数据分块写入
- 生成布隆过滤器以加速查找
- 构建稀疏索引
- 写入文件元信息（Footer）

#### 核心方法：
- `flush(MemTable memTable, String filePath)`: 刷写内存表到磁盘

### 4. SSTableReader（SSTable读取器）

**类**: [SSTableReader.java](src/main/java/com/mini/paimon/storage/SSTableReader.java)

负责从磁盘上的 SSTable 文件中读取数据。

#### 读取流程：
```
Query Key ──▶ Bloom Filter ──▶ Index Block ──▶ Data Block ──▶ Result
              (快速排除)        (定位Block)      (二分查找)
```

#### 核心方法：
- `get(String filePath, RowKey key)`: 根据键获取数据
- `scan(String filePath, RowKey startKey, RowKey endKey, ScanCallback callback)`: 范围扫描

### 5. LSMTree（LSM树主类）

**类**: [LSMTree.java](src/main/java/com/mini/paimon/storage/LSMTree.java)

LSM Tree 的主入口，协调内存表和磁盘表的操作。

#### 工作流程：
1. 数据首先写入活跃内存表（Active MemTable）
2. 当内存表达到大小限制时，转为不可变内存表（Immutable MemTable）
3. 不可变内存表在后台刷写到磁盘生成 SSTable
4. 查询时按顺序检查：活跃内存表 → 不可变内存表 → 磁盘 SSTable

#### 核心方法：
- `put(Row row)`: 插入数据
- `get(RowKey key)`: 获取数据
- `close()`: 关闭并刷写所有数据

## 使用示例

### 基本使用

```java
// 1. 创建表结构
Field idField = new Field("id", DataType.INT, false);
Field nameField = new Field("name", DataType.STRING, true);
Schema schema = new Schema(0, Arrays.asList(idField, nameField), 
                          Collections.singletonList("id"));

// 2. 创建路径工厂和 LSM Tree
PathFactory pathFactory = new PathFactory("./warehouse");
pathFactory.createTableDirectories("example_db", "user_table");
LSMTree lsmTree = new LSMTree(schema, pathFactory, "example_db", "user_table");

// 3. 插入数据
Row row = new Row(new Object[]{1, "Alice"});
lsmTree.put(row);

// 4. 查询数据
RowKey key = RowKey.fromRow(row, schema);
Row result = lsmTree.get(key);

// 5. 关闭 LSM Tree
lsmTree.close();
```

## 测试验证

### 单元测试

- [MemTableTest.java](src/test/java/com/minipaimon/storage/MemTableTest.java): 测试内存表功能
- [LSMTreeTest.java](src/test/java/com/minipaimon/storage/LSMTreeTest.java): 测试 LSM Tree 集成功能

### 测试结果

```
[INFO] Tests run: 20, Failures: 0, Errors: 0, Skipped: 0
```

所有测试均通过，验证了实现的正确性。

## 性能特点

### 优势
1. **写优化**: 写操作只涉及内存和顺序磁盘写入，性能优异
2. **有序存储**: 数据按键排序，支持高效的范围查询
3. **空间效率**: 通过 Compaction 合并数据，减少空间碎片
4. **并发支持**: 支持多线程并发读写

### 当前实现的简化
1. **单层结构**: 当前实现只有一层 SSTable，没有多层合并
2. **简单 Compaction**: 没有实现复杂的合并策略
3. **基础索引**: 使用简单的稀疏索引，没有多级索引

## 文件持久化

LSM Tree 实现了完整的文件持久化功能：

1. **文件路径管理**: 使用 [PathFactory](src/main/java/com/mini/paimon/utils/PathFactory.java) 统一管理文件路径
2. **SSTable 文件**: 数据持久化到磁盘文件，格式为 `data-{level}-{sequence}.sst`
3. **目录结构**: 遵循标准的 Paimon 目录布局

示例文件路径：
```
warehouse/
└── example_db/
    └── user_table/
        └── data/
            └── data-0-000.sst
```

## 下一步改进

1. **实现 Compaction 机制**: 多层 SSTable 合并
2. **WAL（Write-Ahead Log）支持**: 提供数据持久性保证
3. **并发控制优化**: 改进读写并发性能
4. **缓存机制**: 添加 Block Cache 提升读取性能
5. **压缩算法**: 支持数据压缩减少存储空间

## 总结

Mini Paimon 的 LSM Tree 实现提供了核心的存储功能：
- ✅ 内存表管理（MemTable）
- ✅ 磁盘持久化（SSTable）
- ✅ 数据刷写（SSTableWriter）
- ✅ 数据读取（SSTableReader）
- ✅ 完整的 LSM Tree 协调机制
- ✅ 文件持久化到指定目录
- ✅ 完善的测试覆盖

该实现为学习和理解 LSM Tree 的工作原理提供了良好的基础。
