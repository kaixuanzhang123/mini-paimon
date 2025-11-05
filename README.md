# Mini Paimon 项目设计文档

本文档将按步骤拆解一个 Mini Paimon 项目的实现流程，目标是模仿 Apache Paimon 的核心能力，但功能上简化，方便学习 LSM Tree 存储结构、索引管理、表元数据管理、Snapshot/Manifest 文件以及简单 SQL 解析与执行。

## 1. 项目总体结构设计

Mini Paimon 将分为以下模块：

1. **存储层**：实现简化版 LSM Tree，用于数据文件存储与索引。
2. **元数据管理层**：管理表定义（Schema）、表配置以及表的生命周期。
3. **文件管理层**：模仿 Paimon 的 Snapshot 和 Manifest 文件结构及数据写入流程。
4. **SQL 层**：简单的 SQL 解析与执行（支持 INSERT、SELECT）。
5. **工具与辅助模块**：日志、配置、测试工具。

### 阶段一：基础设施搭建

#### 任务 1.1：项目结构初始化

**目标**：建立项目基本框架

- 创建 Maven/Gradle 项目
- 定义包结构：

```
com.minipaimon
├── storage        # 存储引擎
├── metadata       # 元数据管理
├── manifest       # Manifest 文件管理
├── snapshot       # Snapshot 管理
├── index          # 索引实现
├── sql            # SQL 解析器
└── utils          # 工具类
```

- 添加依赖：Guava、Jackson (JSON序列化)、ANTLR (SQL解析)

#### 任务 1.2：基础数据结构定义

**目标**：定义核心数据模型

- 实现 DataType 枚举 (INT, LONG, STRING, BOOLEAN)
- 实现 Field 类（字段名、类型、是否可空）
- 实现 Schema 类（字段列表、主键定义）
- 实现 Row 类（数据行的表示）
- 实现 RowKey 类（主键的序列化表示）

代码示例：

```java
public class Schema {
    private List<Field> fields;
    private List<String> primaryKeys;
    private int schemaId;
}
```

## 2. LSM Tree 存储引擎

### 概述

LSM Tree（Log-Structured Merge Tree）是 Paimon 存储引擎的核心数据结构。本阶段将实现一个简化版的 LSM Tree，包含内存表、持久化文件格式、读写流程和压缩机制。

### 任务 2.1：MemTable 实现

#### 2.1.1 核心数据结构

**MemTable 类设计**

```
MemTable
├── ConcurrentSkipListMap<RowKey, Row>  // 主存储
├── AtomicLong memorySize               // 当前内存占用
├── long createTimestamp                // 创建时间戳
├── long sequenceNumber                 // 序列号
└── ReentrantReadWriteLock lock         // 读写锁
```

**关键配置参数**

- memTableSize: 64MB（默认值）
- flushThreshold: 0.8（触发 flush 的阈值比例）
- maxMemTables: 3（最多同时存在的 MemTable 数量）

#### 2.1.2 核心方法实现细节

**put(RowKey, Row) 方法**

*参数校验*
- 检查 RowKey 是否为 null
- 检查 Row 数据完整性

*写入流程*
- 先写 WAL（Write-Ahead Log）
- 计算 Row 的内存占用大小（包括 key + value + metadata）
- 使用 ConcurrentSkipListMap 插入数据
- 更新 memorySize 计数器
- 如果是更新操作，需要减去旧值的大小

*触发 flush 检查*
- 判断当前内存占用是否超过阈值
- 如果超过，标记当前 MemTable 为 Immutable
- 创建新的 Active MemTable
- 触发异步 flush 任务

**get(RowKey) 方法**

*查询流程*
- 先在 Active MemTable 中查找
- 如果未找到，依次查询 Immutable MemTables
- 返回最新版本的数据（根据 sequenceNumber）

*并发控制*
- 使用读锁保护查询操作
- 避免在 flush 过程中数据不一致

*内存占用计算*

```
Row Memory Size = 
  + RowKey size (字节数组长度)
  + Column values size (所有列值的字节数)
  + Metadata size (时间戳、序列号等)
  + SkipList node overhead (~40 bytes per entry)
```

#### 2.1.3 WAL (Write-Ahead Log) 实现

**WAL 文件格式**

```
┌────────────────────────────────┐
│  Magic Number (4 bytes)        │
├────────────────────────────────┤
│  Version (4 bytes)             │
├────────────────────────────────┤
│  Record 1:                     │
│    - Length (4 bytes)          │
│    - Type (1 byte)             │ ← PUT/DELETE
│    - RowKey (variable)         │
│    - Row Data (variable)       │
│    - Checksum (4 bytes)        │
├────────────────────────────────┤
│  Record 2...                   │
└────────────────────────────────┘
```

**WAL 操作流程**

*写入*
- 追加写入（Append-Only）
- 每条记录包含 CRC32 校验和
- 定期 fsync 刷盘（可配置间隔）

*恢复*
- 启动时扫描 WAL 文件
- 按顺序重放所有记录
- 重建 MemTable 状态
- 删除已 flush 的 WAL 文件

*文件管理*
- 命名规则：wal-{sequenceNumber}.log
- 每个 MemTable 对应一个 WAL 文件
- Flush 完成后删除对应 WAL

#### 2.1.4 MemTable 状态管理

**状态转换**

```
ACTIVE → IMMUTABLE → FLUSHING → FLUSHED
```

- ACTIVE: 可写入状态
- IMMUTABLE: 只读状态，等待 flush
- FLUSHING: 正在写入磁盘
- FLUSHED: 已持久化，可以删除

### 任务 2.2：SSTable 文件格式设计

#### 2.2.1 整体文件结构

```
┌────────────────────────────────┐  Offset: 0
│    File Header (32 bytes)      │
│  - Magic Number                │
│  - Version                     │
│  - Compression Type            │
│  - Create Timestamp            │
├────────────────────────────────┤
│    Data Block 1 (4KB)          │
│  ┌──────────────────────────┐  │
│  │ Restart Points Array     │  │
│  │ Entry 1: key1, value1    │  │
│  │ Entry 2: key2, value2    │  │
│  │ ...                      │  │
│  │ Block Footer             │  │
│  └──────────────────────────┘  │
├────────────────────────────────┤
│    Data Block 2...             │
├────────────────────────────────┤
│    Meta Block                  │
│  - Statistics (min/max key)    │
│  - Column Schema               │
├────────────────────────────────┤
│    Index Block                 │
│  ┌──────────────────────────┐  │
│  │ Entry 1:                 │  │
│  │   - First Key            │  │
│  │   - Block Offset         │  │
│  │   - Block Size           │  │
│  │ Entry 2...               │  │
│  └──────────────────────────┘  │
├────────────────────────────────┤
│    Bloom Filter Block          │
│  - Bit Array                   │
│  - Hash Function Count         │
├────────────────────────────────┤
│    Footer (48 bytes)           │
│  - Meta Block Handle           │
│  - Index Block Handle          │
│  - Bloom Filter Handle         │
│  - Row Count                   │
│  - Checksum                    │
└────────────────────────────────┘  End of File
```

#### 2.2.2 Data Block 详细格式

**数据压缩与编码**

*前缀压缩*
- 相邻 key 通常有公共前缀
- 只存储不同的后缀部分
- 设置 Restart Point（每 16 个 key）

*Delta Encoding*
- 对数值类型列使用差值编码
- 减少存储空间

**Data Block 内部结构**

*Entry Format:*

```
┌────────────────────────────────┐
│ Shared Key Length (varint)     │ ← 与前一个 key 的公共前缀长度
│ Unshared Key Length (varint)   │ ← 非公共部分长度
│ Value Length (varint)           │
│ Unshared Key Data              │
│ Value Data                     │
└────────────────────────────────┘
```

*Restart Points Array:*

```
[offset1, offset2, ..., offsetN, restart_count]
```

#### 2.2.3 Index Block 设计

**稀疏索引结构**

*Index Entry:*

```
┌────────────────────────────────┐
│ Separator Key                  │ ← 分隔键（>= last key of block i, < first key of block i+1)
│ Block Offset (8 bytes)         │
│ Block Size (4 bytes)           │
│ First Key Length (4 bytes)     │
│ First Key Data                 │
└────────────────────────────────┘
```

**索引优化策略**
- 使用最短的分隔键减少索引大小
- 支持二分查找快速定位 Block
- 加载时缓存在内存中

#### 2.2.4 Bloom Filter Block

**Bloom Filter 配置**
- Bits per Key: 10（误判率约 1%）
- Hash Function Count: 7
- 实现: Murmur3 Hash

**存储格式**

```
┌────────────────────────────────┐
│ Bits per Key (4 bytes)         │
│ Hash Count (4 bytes)           │
│ Total Bits (8 bytes)           │
│ Bit Array (variable)           │
└────────────────────────────────┘
```

#### 2.2.5 Footer 格式

**Footer (48 bytes fixed):**

```
┌────────────────────────────────┐
│ Meta Block Offset (8 bytes)    │
│ Meta Block Size (4 bytes)      │
│ Index Block Offset (8 bytes)   │
│ Index Block Size (4 bytes)     │
│ Bloom Filter Offset (8 bytes)  │
│ Bloom Filter Size (4 bytes)    │
│ Row Count (8 bytes)            │
│ Magic Number (4 bytes)         │
└────────────────────────────────┘
```

### 任务 2.3：SSTable Writer

#### 2.3.1 SSTableWriter 类设计

```
SSTableWriter
├── FileChannel outputChannel
├── DataBlockBuilder currentBlock
├── List<IndexEntry> indexEntries
├── BloomFilterBuilder bloomFilter
├── long currentOffset
└── Statistics stats (min/max key, row count)
```

#### 2.3.2 Flush 流程

**整体流程**

```
MemTable (Immutable)
    ↓
1. 创建 SSTableWriter
    ↓
2. 遍历有序数据
    ↓
3. 写入 Data Blocks
    ↓
4. 构建 Index
    ↓
5. 生成 Bloom Filter
    ↓
6. 写入 Footer
    ↓
7. 关闭文件
    ↓
8. 注册到 Manifest
```

**详细步骤**

*Step 1: 初始化*
- 创建临时文件（.tmp 后缀）
- 初始化 DataBlockBuilder（4KB 缓冲区）
- 初始化 BloomFilterBuilder
- 写入 File Header

*Step 2: 写入数据行*

```
for each (key, row) in MemTable:
    1. 添加 key 到 Bloom Filter
    2. 序列化 row 数据
    3. 添加到 currentBlock
    4. 如果 block 满了（>= 4KB）:
        a. 压缩 block（可选：Snappy/LZ4）
        b. 计算 checksum
        c. 写入磁盘
        d. 记录 index entry（first key, offset, size）
        e. 重置 currentBlock
```

*Step 3: 写入 Meta Block*
- 统计信息（min/max key, row count）
- Schema 信息
- Compression 类型

*Step 4: 写入 Index Block*
- 将所有 IndexEntry 序列化
- 写入磁盘
- 记录 offset 和 size

*Step 5: 写入 Bloom Filter*
- 序列化 bit array
- 写入磁盘
- 记录 offset 和 size

*Step 6: 写入 Footer*
- 填充所有 block handles
- 计算文件 checksum
- 写入固定 48 字节

*Step 7: 原子重命名*
- fsync 确保数据落盘
- 将 .tmp 文件重命名为 .sst

#### 2.3.3 文件命名规则

**格式**: {level}-{sequence}-{uuid}.sst

**例如**:
- 0-00001-a1b2c3d4.sst  (Level 0, 序列号 1)
- 1-00025-e5f6g7h8.sst  (Level 1, 序列号 25)

#### 2.3.4 并发控制

- 每个 MemTable flush 使用独立线程
- 使用 Semaphore 限制并发 flush 数量
- Flush 过程中不阻塞新写入

### 任务 2.4：SSTable Reader

#### 2.4.1 SSTableReader 类设计

```
SSTableReader
├── FileChannel inputChannel
├── MappedByteBuffer fileMapping (可选)
├── Footer footer
├── IndexBlock indexBlock (cached in memory)
├── BloomFilter bloomFilter (cached in memory)
├── LRUCache<BlockOffset, DataBlock> blockCache
└── Statistics stats
```

#### 2.4.2 初始化流程

```
1. 打开文件
   ↓
2. 读取 Footer (从文件末尾倒数 48 字节)
   ↓
3. 验证 Magic Number
   ↓
4. 读取并缓存 Index Block
   ↓
5. 读取并缓存 Bloom Filter
   ↓
6. 准备就绪
```

#### 2.4.3 点查询 get(RowKey) 实现

**完整查询流程**

```
Query: get(key="user123")
    ↓
Step 1: Bloom Filter 检查
    if NOT bloom.mightContain(key):
        return null (快速排除)
    ↓
Step 2: Index Block 二分查找
    找到包含 key 的 Data Block
    (定位到 block offset 和 size)
    ↓
Step 3: 读取 Data Block
    先检查 Block Cache
    if cache miss:
        从文件读取 block
        解压缩（如果需要）
        加入 cache
    ↓
Step 4: Data Block 内二分查找
    使用 Restart Points 加速
    找到目标 entry
    ↓
Step 5: 反序列化 Row
    返回结果
```

**优化技巧**

*Block Cache*
- LRU 策略
- 默认大小：256MB
- 缓存解压后的 block

*预读取（Read-Ahead）*
- 扫描场景下预读相邻 block
- 减少 IO 次数

*内存映射（mmap）*
- 小文件使用 MappedByteBuffer

## 3. 索引系统

### 任务 3.1：Bloom Filter 索引

**目标**：快速判断 Key 是否存在
- 使用 Guava 的 BloomFilter
- 为每个 SSTable 创建独立的 Bloom Filter
- 设置合理的误判率（1%）
- 序列化到 SSTable 文件

### 任务 3.2：稀疏索引实现

**目标**：快速定位数据块
- 实现 IndexEntry 类（Key + Offset）
- 在写入时每个 DataBlock 记录一个索引项
- 加载时将索引加载到内存
- 实现二分查找定位算法

### 任务 3.3：数据块索引实现

1. **实现 Compaction 机制**: 多层 SSTable 合并
2. **WAL（Write-Ahead Log）支持**: 提供数据持久性保证
3. **并发控制优化**: 改进读写并发性能
4. **缓存机制**: 添加 Block Cache 提升读取性能
5. **压缩算法**: 支持数据压缩减少存储空间

## 4. 元数据管理

### 概述

元数据管理是 Paimon 的核心组件之一，负责管理表的结构定义、版本演化和持久化。本方案主要实现 Schema 管理和 Table 元数据管理两大模块。

### 任务 1：Table Schema 管理

#### 1.1 Schema 数据结构设计

**Schema 类**

核心字段：
- schemaId: long - Schema 版本号，从 0 开始递增
- fields: List<Field> - 字段列表
- primaryKeys: List<String> - 主键字段名列表
- partitionKeys: List<String> - 分区键字段名列表
- options: Map<String, String> - 表级别配置选项
- comment: String - 表注释（可选）
- timeMillis: long - Schema 创建时间戳

**Field 类**

核心字段：
- id: int - 字段唯一标识符，用于 Schema 演化
- name: String - 字段名称
- type: DataType - 字段数据类型
- nullable: boolean - 是否允许为空
- description: String - 字段描述（可选）
- defaultValue: Object - 默认值（可选）

**DataType 枚举/类**

支持的基本类型：
- INT (4字节整数)
- BIGINT (8字节长整数)
- FLOAT (4字节浮点数)
- DOUBLE (8字节双精度浮点数)
- STRING (变长字符串)
- BOOLEAN (布尔值)

#### 1.2 SchemaManager 类实现

**核心职责**
- Schema 的序列化和反序列化
- Schema 版本管理
- Schema 读取和缓存
- Schema 演化验证

**主要方法**

**createSchema**
- 功能：创建新的 Schema
- 输入：TableSchema, tablePath
- 流程：
  1. 分配新的 schemaId（读取当前最大 ID + 1）
  2. 验证 Schema 合法性（主键存在性、字段名唯一性等）
  3. 序列化为 JSON
  4. 写入文件 `{tablePath}/schema/schema-{schemaId}`
  5. 更新Schema ID
- 输出：schemaId

**commitSchema**
- 功能：提交 Schema 演化
- 输入：newSchema, oldSchemaId, tablePath
- 流程：
  1. 读取旧 Schema
  2. 验证 Schema 演化兼容性
  3. 分配新 schemaId
  4. 持久化新 Schema

**getSchema**
- 功能：根据 ID 获取 Schema
- 输入：schemaId, tablePath
- 流程：
  1. 检查内存缓存
  2. 如果缓存未命中，从文件读取
  3. 反序列化 JSON
  4. 更新缓存
  5. 返回 Schema 对象

**listSchemas**
- 功能：列出所有历史 Schema 版本
- 输入：tablePath
- 输出：List<Long> - 所有 schemaId 列表

#### 1.3 Schema 文件结构

**目录布局**

```
{tablePath}/
├── schema/
│   ├── schema-0          # 初始 Schema
│   ├── schema-1          # 第一次演化后的 Schema
│   ├── schema-2          # 第二次演化后的 Schema
│  
```

#### 1.4 Schema 演化管理

**演化规则**

允许的演化操作：
- 添加新字段（必须是 nullable 或有默认值）
- 修改字段注释
- 修改表注释
- 添加表选项

禁止的演化操作：
- 删除字段
- 修改字段类型
- 修改字段 ID
- 修改主键定义
- 修改分区键定义
- 将 nullable 字段改为 non-nullable

**演化验证逻辑**
`validateSchemaEvolution(oldSchema, newSchema)`:
1. 检查主键是否变化
2. 检查分区键是否变化
3. 遍历旧 Schema 的字段：
   - 验证字段是否存在于新 Schema
   - 验证字段 ID 未改变
   - 验证字段类型未改变
   - 验证 nullable 属性未变严格
4. 遍历新增字段：
   - 验证必须是 nullable 或有默认值
   - 分配新的 field ID（使用最大 ID + 1）
5. 所有检查通过才允许提交

**Field ID 管理**

字段 ID 的作用：
- 在 Schema 演化时保持字段的稳定标识
- 即使字段重命名，Field ID 保持不变
- 用于数据文件的列映射

分配策略：
- 初始 Schema：按字段顺序从 0 开始
- Schema 演化：新字段 ID = max(existing field IDs) + 1
- 删除字段后 ID 不复用（避免混淆）

### 任务 2：Table 元数据管理

#### 2.2 SchemaManager 类实现

**主要方法**

**createTable**
- 功能：创建新表的元数据
- 输入：Schema schema
- 流程：
  1. 生成 tableUUID
  2. 设置创建时间和修改时间
  3. 初始化 snapshotId 为 -1
  4. 序列化为 JSON
  5. 写入 `{tablePath}/metadata` 文件
  6. 返回 TableSchema 对象

**updateTable**
- 功能：更新表元数据（Schema）
- 输入：updated Schema
- 流程：
  1. 对比旧 Schema，校验schema是否合法
  2. 更新相关字段（schemaId等）
  3. 原子性写入新文件（写临时文件 + rename）

#### 2.4 元数据持久化策略

**原子性写入**

实现方式：
1. 创建临时文件：`{tablePath}/metadata.tmp.{timestamp}`
2. 写入完整的 JSON 内容
3. 调用 fsync 确保刷盘
4. 原子重命名：`metadata.tmp.{timestamp}` -> `metadata`
5. 删除旧的临时文件（如果存在）

优点：
- 保证元数据文件的完整性
- 避免写入过程中的文件损坏
- 支持并发安全

## 5. Snapshot 机制

### 任务 5.1：Snapshot 设计

**目标**：实现版本控制

**Snapshot 文件结构**：

```json
{
  "snapshotId": 1,
  "schemaId": 0,
  "commitTime": 1699000000,
  "manifestList": "manifest/manifest-list-1"
}
```

**Timeline**:

```
snapshot-1 ──▶ snapshot-2 ──▶ snapshot-3 (LATEST)
    │              │              │
    ▼              ▼              ▼
manifest-1     manifest-2     manifest-3
```

- 实现 Snapshot 类
- 文件命名：snapshot/snapshot-{id}
- 维护 LATEST 指针（软链接或标记文件）
- 实现快照的创建和读取

### 任务 5.2：Manifest 文件

**目标**：追踪数据文件变更

**Manifest List 格式**：

```json
{
  "manifestFiles": [
    "manifest/manifest-abc123",
    "manifest/manifest-def456"
  ]
}
```

**Manifest File 格式**：

```json
{
  "entries": [
    {
      "kind": "ADD",
      "file": "data/data-0-001.sst",
      "level": 0,
      "minKey": "...",
      "maxKey": "...",
      "rowCount": 1000
    }
  ]
}
```

- 实现 ManifestEntry 类（ADD/DELETE）
- 实现 ManifestFile 类
- 实现 ManifestList 类
- 写入时增量记录文件变更
- 读取时合并所有 Manifest 得到当前文件列表

### 任务 5.3：快照读取流程

**目标**：实现时间旅行查询

**Read Flow**:

```
Query ──▶ Load Latest Snapshot ──▶ Read Manifest ──▶ Get File List
                                                          │
                                                          ▼
                                    Merge Read from Multiple SSTables
```

- 根据 Snapshot ID 加载对应版本
- 解析 Manifest 获取所有活跃文件
- 合并多个 SSTable 的查询结果（按 Key 去重，取最新）

## 6. 文件组织结构

### 任务 6.1：目录结构设计

**目标**：参考 Paimon 的目录布局

```
warehouse/
└── {database}/
    └── {table}/
        ├── schema/
        │   ├── schema-0
        │   └── schema-1
        ├── snapshot/
        │   ├── snapshot-1
        │   ├── snapshot-2
        │   └── LATEST
        ├── manifest/
        │   ├── manifest-list-1
        │   ├── manifest-abc123
        │   └── manifest-def456
        └── data/
            ├── data-0-001.sst
            ├── data-0-002.sst
            └── data-1-001.sst
```

- 实现 PathFactory 工具类
- 自动创建目录结构
- 实现文件清理（旧快照的垃圾回收）

## 7. LSM Tree 与 Snapshot 集成

**目标**：将 LSM Tree 存储引擎与 Snapshot 机制集成

- 修改 LSMTree 类，在数据刷写时创建快照
- 实现数据文件变更的自动记录到 Manifest
- 验证快照机制的完整性和正确性
- 提供时间旅行查询能力

## 8. 通过 SQL 读写数据流程

### 一、项目整体架构

#### 1.1 核心模块划分

- **SQL解析模块**：负责解析CREATE TABLE和INSERT语句
- **Schema管理模块**：负责表结构的创建、存储和检索
- **数据写入模块**：负责将数据写入Snapshot文件
- **元数据管理模块**：负责管理表的元数据信息
- **文件存储模块**：负责底层文件的读写操作

### 二、CREATE TABLE 功能实现流程

#### 2.1 SQL解析阶段

**词法分析**
- 将CREATE TABLE SQL语句分解为Token序列
- 识别关键字：CREATE, TABLE, 列名, 数据类型, 约束等

**语法分析**
- 构建抽象语法树(AST)
- 提取表名、列定义、主键、分区键等信息
- 验证SQL语法的正确性

**语义分析**
- 检查表是否已存在
- 验证数据类型的合法性
- 验证约束条件的合理性

#### 2.2 Schema生成阶段

**Schema对象构建**
- 创建TableSchema对象
- 包含字段：
  - 表名(tableName)
  - 列信息列表(columns)：列名、数据类型、是否可空、默认值
  - 主键信息(primaryKeys)
  - 分区键信息(partitionKeys)
  - 表属性(properties)：如存储格式、压缩方式等
  - Schema版本号(schemaId)
  - 创建时间戳

**Schema验证**
- 验证主键列必须存在于列定义中
- 验证分区键的合法性
- 检查列名是否重复

#### 2.3 Schema持久化阶段

**文件路径规划**
- 目录结构：{warehouse_path}/{database}/{table}/schema/
- 文件命名：schema-{schemaId}.json 或 schema-{schemaId}.avro

**序列化Schema**
- 将Schema对象序列化为JSON或Avro格式
- 包含完整的表结构信息

**写入磁盘**
- 使用原子写操作确保数据完整性
- 先写临时文件，再重命名为正式文件
- 更新最新Schema指针文件(latest-schema)

**元数据注册**
- 在元数据目录中记录表的基本信息
- 建立表名到Schema文件的映射关系

### 三、INSERT 功能实现流程

#### 3.1 SQL解析阶段

**词法和语法分析**
- 解析INSERT INTO语句
- 提取目标表名
- 提取列名列表(如果指定)
- 提取VALUES子句中的数据值

**数据提取**
- 将VALUES中的数据解析为行记录
- 支持多行插入：INSERT INTO table VALUES (row1), (row2), ...
- 处理数据类型转换

#### 3.2 Schema查找与验证阶段

**定位Schema文件**
- 根据表名查找对应的Schema目录
- 读取latest-schema指针，获取最新的Schema版本
- 加载Schema文件到内存

**数据验证**
- 验证插入的列数与Schema定义是否匹配
- 验证数据类型是否兼容
- 检查NOT NULL约束
- 验证主键唯一性(可选，简化版本可暂不实现)

**数据转换与填充**
- 如果INSERT语句未指定列名，按Schema顺序填充
- 如果指定了列名，按映射关系填充数据
- 为未指定的列填充默认值或NULL

#### 3.3 数据写入Snapshot阶段

**确定写入位置**
- 目录结构：{warehouse_path}/{database}/{table}/snapshot/
- 根据分区键计算分区路径(如有分区)

**构建Snapshot对象**
- 生成唯一的Snapshot ID(递增或时间戳)
- 记录写入信息：
  - commitId：提交标识
  - schemaId：使用的Schema版本
  - timestamp：提交时间
  - operation：操作类型(INSERT)
  - dataFilesList：数据文件列表

**数据文件写入**
- 文件格式选择：
  - 简单实现：JSON格式，每行一个JSON对象
  - 进阶实现：Parquet或ORC等列式存储格式
- 数据文件命名：
  - data-{commitId}-{fileId}.{format}
  - 例如：data-000001-001.json
- 写入流程：
  - 将验证后的数据行序列化
  - 按批次写入数据文件
  - 计算数据文件的统计信息(行数、文件大小等)

**Snapshot元数据写入**
- Snapshot文件命名：snapshot-{snapshotId}.json
- Snapshot内容：

```json
{
  "snapshotId": 1,
  "schemaId": 0,
  "commitId": 1,
  "timestamp": 1699123456789,
  "operation": "INSERT",
  "dataFiles": [
    {
      "path": "data-000001-001.json",
      "rowCount": 100,
      "fileSize": 1024,
      "minValues": {...},
      "maxValues": {...}
    }
  ],
  "summary": {
    "totalRecords": 100,
    "totalFiles": 1
  }
}
```

**更新Snapshot链**
- 更新latest-snapshot指针指向新的Snapshot
- 维护Snapshot的版本链(可选，用于时间旅行功能)

### 四、关键数据结构设计

#### 4.1 Schema相关

**TableSchema:**
- tableName: String
- schemaId: Long
- columns: List<Column>
- primaryKeys: List<String>
- partitionKeys: List<String>
- properties: Map<String, String>
- createTime: Long

**Column:**
- name: String
- dataType: DataType
- nullable: Boolean
- defaultValue: Object
- comment: String

#### 4.2 Snapshot相关

**Snapshot:**
- snapshotId: Long
- schemaId: Long
- commitId: Long
- timestamp: Long
- operation: OperationType
- dataFiles: List<DataFile>
- summary: CommitSummary

**DataFile:**
- path: String
- rowCount: Long
- fileSize: Long
- minValues: Map<String, Object>
- maxValues: Map<String, Object>

### 五、目录结构设计

```
warehouse/
├── {database}/
│   └── {table}/
│       ├── schema/
│       │   ├── schema-0.json
│       │   ├── schema-1.json
│       │   └── latest-schema (指向最新schema)
│       ├── snapshot/
│       │   ├── snapshot-1.json
│       │   ├── snapshot-2.json
│       │   └── latest-snapshot (指向最新snapshot)
│       └── data/
│           ├── data-000001-001.json
│           ├── data-000002-001.json
│           └── ...
```

### 六、实现建议

#### 6.1 第一阶段：基础功能

- 实现简单的SQL解析器(可使用正则表达式或手写Parser)
- 支持基本数据类型：INT, BIGINT, STRING, DOUBLE, BOOLEAN
- 使用JSON格式存储Schema和数据文件
- 单线程、无并发控制

#### 6.2 第二阶段：功能增强

- 引入SQL解析库(如Apache Calcite、JSqlParser)
- 支持更多数据类型和复杂表达式
- 添加数据校验和类型转换
- 实现基本的错误处理机制

#### 6.3 第三阶段：性能优化

- 使用高效的存储格式(Parquet、ORC)
- 实现批量写入优化
- 添加统计信息收集
- 实现简单的并发控制

### 七、测试建议

#### 7.1 单元测试

- SQL解析器测试：各种SQL语法
- Schema序列化/反序列化测试
- 数据类型转换测试
- 文件读写测试

#### 7.2 集成测试

- 端到端CREATE TABLE测试
- 端到端INSERT测试
- Schema版本升级测试
- 多次INSERT后的数据一致性测试

#### 7.3 边界测试

- 空表插入
- 大批量数据插入
- 异常SQL语句处理
- 文件系统异常处理