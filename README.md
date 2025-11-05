本文档将按步骤拆解一个 Mini Paimon 项目的实现流程，目标是模仿 Apache Paimon 的核心能力，但功能上简化，方便学习 LSM Tree 存储结构、索引管理、表元数据管理、Snapshot/Manifest 文件以及简单 SQL 解析与执行。

1. 项目总体结构设计
Mini Paimon 将分为以下模块：
1. 存储层：实现简化版 LSM Tree，用于数据文件存储与索引。
2. 元数据管理层：管理表定义（Schema）、表配置以及表的生命周期。
3. 文件管理层：模仿 Paimon 的 Snapshot 和 Manifest 文件结构及数据写入流程。
4. SQL 层：简单的 SQL 解析与执行（支持 INSERT、SELECT）。
5. 工具与辅助模块：日志、配置、测试工具。

阶段一：基础设施搭建
任务 1.1：项目结构初始化
目标：建立项目基本框架
●  创建 Maven/Gradle 项目
●  定义包结构：
com.minipaimon
├── storage        # 存储引擎
├── metadata       # 元数据管理
├── manifest       # Manifest 文件管理
├── snapshot       # Snapshot 管理
├── index          # 索引实现
├── sql            # SQL 解析器
└── utils          # 工具类
●  添加依赖：Guava、Jackson (JSON序列化)、ANTLR (SQL解析)
任务 1.2：基础数据结构定义
目标：定义核心数据模型
●  实现 DataType 枚举 (INT, LONG, STRING, BOOLEAN)
●  实现 Field 类（字段名、类型、是否可空）
●  实现 Schema 类（字段列表、主键定义）
●  实现 Row 类（数据行的表示）
●  实现 RowKey 类（主键的序列化表示）
代码示例：
java


public class Schema {
    private List<Field> fields;
    private List<String> primaryKeys;
    private int schemaId;
}
阶段二：LSM Tree 存储引擎
概述
LSM Tree（Log-Structured Merge Tree）是 Paimon 存储引擎的核心数据结构3。本阶段将实现一个简化版的 LSM Tree，包含内存表、持久化文件格式、读写流程和压缩机制。

任务 2.1：MemTable 实现
2.1.1 核心数据结构
MemTable 类设计
MemTable
├── ConcurrentSkipListMap<RowKey, Row>  // 主存储
├── AtomicLong memorySize               // 当前内存占用
├── long createTimestamp                // 创建时间戳
├── long sequenceNumber                 // 序列号
└── ReentrantReadWriteLock lock         // 读写锁
关键配置参数
memTableSize: 64MB（默认值）
flushThreshold: 0.8（触发 flush 的阈值比例）
maxMemTables: 3（最多同时存在的 MemTable 数量）
2.1.2 核心方法实现细节
put(RowKey, Row) 方法
参数校验

检查 RowKey 是否为 null
检查 Row 数据完整性
写入流程

先写 WAL（Write-Ahead Log）
计算 Row 的内存占用大小（包括 key + value + metadata）
使用 ConcurrentSkipListMap 插入数据
更新 memorySize 计数器
如果是更新操作，需要减去旧值的大小
触发 flush 检查

判断当前内存占用是否超过阈值
如果超过，标记当前 MemTable 为 Immutable
创建新的 Active MemTable
触发异步 flush 任务
get(RowKey) 方法
查询流程

先在 Active MemTable 中查找
如果未找到，依次查询 Immutable MemTables
返回最新版本的数据（根据 sequenceNumber）
并发控制

使用读锁保护查询操作
避免在 flush 过程中数据不一致
内存占用计算
Row Memory Size = 
  + RowKey size (字节数组长度)
  + Column values size (所有列值的字节数)
  + Metadata size (时间戳、序列号等)
  + SkipList node overhead (~40 bytes per entry)
2.1.3 WAL (Write-Ahead Log) 实现
WAL 文件格式
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
WAL 操作流程
写入

追加写入（Append-Only）
每条记录包含 CRC32 校验和
定期 fsync 刷盘（可配置间隔）
恢复

启动时扫描 WAL 文件
按顺序重放所有记录
重建 MemTable 状态
删除已 flush 的 WAL 文件
文件管理

命名规则：wal-{sequenceNumber}.log
每个 MemTable 对应一个 WAL 文件
Flush 完成后删除对应 WAL
2.1.4 MemTable 状态管理
状态转换
ACTIVE → IMMUTABLE → FLUSHING → FLUSHED
ACTIVE: 可写入状态
IMMUTABLE: 只读状态，等待 flush
FLUSHING: 正在写入磁盘
FLUSHED: 已持久化，可以删除
任务 2.2：SSTable 文件格式设计
2.2.1 整体文件结构
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
2.2.2 Data Block 详细格式3
数据压缩与编码
前缀压缩

相邻 key 通常有公共前缀
只存储不同的后缀部分
设置 Restart Point（每 16 个 key）
Delta Encoding

对数值类型列使用差值编码
减少存储空间
Data Block 内部结构
Entry Format:
┌────────────────────────────────┐
│ Shared Key Length (varint)     │ ← 与前一个 key 的公共前缀长度
│ Unshared Key Length (varint)   │ ← 非公共部分长度
│ Value Length (varint)           │
│ Unshared Key Data              │
│ Value Data                     │
└────────────────────────────────┘

Restart Points Array:
[offset1, offset2, ..., offsetN, restart_count]
2.2.3 Index Block 设计
稀疏索引结构
Index Entry:
┌────────────────────────────────┐
│ Separator Key                  │ ← 分隔键（>= last key of block i, < first key of block i+1)
│ Block Offset (8 bytes)         │
│ Block Size (4 bytes)           │
│ First Key Length (4 bytes)     │
│ First Key Data                 │
└────────────────────────────────┘
索引优化策略
使用最短的分隔键减少索引大小
支持二分查找快速定位 Block
加载时缓存在内存中
2.2.4 Bloom Filter Block3
Bloom Filter 配置
Bits per Key: 10（误判率约 1%）
Hash Function Count: 7
实现: Murmur3 Hash
存储格式
┌────────────────────────────────┐
│ Bits per Key (4 bytes)         │
│ Hash Count (4 bytes)           │
│ Total Bits (8 bytes)           │
│ Bit Array (variable)           │
└────────────────────────────────┘
2.2.5 Footer 格式
Footer (48 bytes fixed):
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
任务 2.3：SSTable Writer
2.3.1 SSTableWriter 类设计
SSTableWriter
├── FileChannel outputChannel
├── DataBlockBuilder currentBlock
├── List<IndexEntry> indexEntries
├── BloomFilterBuilder bloomFilter
├── long currentOffset
└── Statistics stats (min/max key, row count)
2.3.2 Flush 流程
整体流程
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
详细步骤
Step 1: 初始化

创建临时文件（.tmp 后缀）
初始化 DataBlockBuilder（4KB 缓冲区）
初始化 BloomFilterBuilder
写入 File Header
Step 2: 写入数据行

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
Step 3: 写入 Meta Block

统计信息（min/max key, row count）
Schema 信息
Compression 类型
Step 4: 写入 Index Block

将所有 IndexEntry 序列化
写入磁盘
记录 offset 和 size
Step 5: 写入 Bloom Filter

序列化 bit array
写入磁盘
记录 offset 和 size
Step 6: 写入 Footer

填充所有 block handles
计算文件 checksum
写入固定 48 字节
Step 7: 原子重命名

fsync 确保数据落盘
将 .tmp 文件重命名为 .sst
2.3.3 文件命名规则
格式: {level}-{sequence}-{uuid}.sst

例如:
- 0-00001-a1b2c3d4.sst  (Level 0, 序列号 1)
- 1-00025-e5f6g7h8.sst  (Level 1, 序列号 25)
2.3.4 并发控制
每个 MemTable flush 使用独立线程
使用 Semaphore 限制并发 flush 数量
Flush 过程中不阻塞新写入
任务 2.4：SSTable Reader3
2.4.1 SSTableReader 类设计
SSTableReader
├── FileChannel inputChannel
├── MappedByteBuffer fileMapping (可选)
├── Footer footer
├── IndexBlock indexBlock (cached in memory)
├── BloomFilter bloomFilter (cached in memory)
├── LRUCache<BlockOffset, DataBlock> blockCache
└── Statistics stats
2.4.2 初始化流程
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
2.4.3 点查询 get(RowKey) 实现
完整查询流程
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
优化技巧
Block Cache

LRU 策略
默认大小：256MB
缓存解压后的 block
预读取（Read-Ahead）

扫描场景下预读相邻 block
减少 IO 次数
内存映射（mmap）

小文件使用 MappedByteBuffer

阶段三：索引系统
任务 3.1：Bloom Filter 索引
目标：快速判断 Key 是否存在
●  使用 Guava 的 BloomFilter
●  为每个 SSTable 创建独立的 Bloom Filter
●  设置合理的误判率（1%）
●  序列化到 SSTable 文件
任务 3.2：稀疏索引实现
目标：快速定位数据块
●  实现 IndexEntry 类（Key + Offset）
●  在写入时每个 DataBlock 记录一个索引项
●  加载时将索引加载到内存
●  实现二分查找定位算法
阶段四：元数据管理
任务 4.1：Table Schema 管理
目标：持久化表结构
Schema 文件格式 (JSON):
json


{
  "schemaId": 0,
  "fields": [
    {"name": "id", "type": "INT", "nullable": false},
    {"name": "name", "type": "STRING", "nullable": true}
  ],
  "primaryKeys": ["id"],
  "partitionKeys": []
}
●  实现 SchemaManager 类
●  Schema 序列化为 JSON 文件：schema/schema-{id}
●  Schema 版本管理（支持演化）
●  实现 Schema 读取和缓存
任务 4.2：Table 元数据
目标：管理表级别信息
●  实现 TableMetadata 类
●  存储表名、Schema ID、创建时间等
●  文件路径：{tableName}/metadata
阶段五：Snapshot 机制
任务 5.1：Snapshot 设计
目标：实现版本控制
Snapshot 文件结构：
json


{
  "snapshotId": 1,
  "schemaId": 0,
  "commitTime": 1699000000,
  "manifestList": "manifest/manifest-list-1"
}
Timeline:
snapshot-1 ──▶ snapshot-2 ──▶ snapshot-3 (LATEST)
    │              │              │
    ▼              ▼              ▼
manifest-1     manifest-2     manifest-3
●  实现 Snapshot 类
●  文件命名：snapshot/snapshot-{id}
●  维护 LATEST 指针（软链接或标记文件）
●  实现快照的创建和读取
任务 5.2：Manifest 文件
目标：追踪数据文件变更
Manifest List 格式：
json


{
  "manifestFiles": [
    "manifest/manifest-abc123",
    "manifest/manifest-def456"
  ]
}
Manifest File 格式：
json


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
●  实现 ManifestEntry 类（ADD/DELETE）
●  实现 ManifestFile 类
●  实现 ManifestList 类
●  写入时增量记录文件变更
●  读取时合并所有 Manifest 得到当前文件列表
任务 5.3：快照读取流程
目标：实现时间旅行查询
Read Flow:
Query ──▶ Load Latest Snapshot ──▶ Read Manifest ──▶ Get File List
                                                          │
                                                          ▼
                                    Merge Read from Multiple SSTables
●  根据 Snapshot ID 加载对应版本
●  解析 Manifest 获取所有活跃文件
●  合并多个 SSTable 的查询结果（按 Key 去重，取最新）
阶段六：文件组织结构
任务 6.1：目录结构设计
目标：参考 Paimon 的目录布局
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
●  实现 PathFactory 工具类
●  自动创建目录结构
●  实现文件清理（旧快照的垃圾回收）