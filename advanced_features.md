# LSM Tree 高级特性

本文档介绍 mini-paimon 存储引擎的三大核心高级特性。

## 1. 非主键表支持

### 设计理念
参考开源 Apache Paimon，支持创建无主键的表。非主键表使用内部自增序列生成唯一键。

### 实现细节
- **Schema 支持**: 允许主键列表为空
- **自动键生成**: MemTable 使用 `AtomicLong` 为每行生成唯一键
- **存储格式**: 与主键表一致，仅键的生成方式不同

### 使用示例
```java
// 创建无主键Schema
Schema schema = new Schema(
    1,
    Arrays.asList(
        new Field("name", DataType.STRING, true),
        new Field("value", DataType.INT, true)
    ),
    Arrays.asList(),  // 空主键列表
    Arrays.asList()
);

LSMTree lsmTree = new LSMTree(schema, pathFactory, "db", "table");
lsmTree.put(new Row(new Object[]{"item1", 100;}))
lsmTree.put(new Row(new Object[]{"item1", 100})); // 允许完全相同的数据
```

## 2. Write-Ahead Log (WAL)

### 设计理念
参考 Apache Paimon 的 WAL 机制，在数据写入内存表之前先记录到磁盘日志，提供崩溃恢复能力。

### 核心组件

#### WriteAheadLog 类
```
warehouse/
└── {database}/
    └── {table}/
        └── wal/
            ├── wal-000.log
            ├── wal-001.log
            └── ...
```

### 工作流程
1. **写入流程**:
   - 接收 `put()` 请求
   - 先写 WAL (`wal.append()`)
   - 再写 MemTable
   - 返回成功

2. **恢复流程**:
   - LSMTree 初始化时读取 WAL
   - 重放日志中的所有操作
   - 恢复内存表状态

3. **WAL 切换**:
   - MemTable flush 时切换新 WAL
   - 删除旧 WAL 文件

### 关键特性
- **持久性保证**: 每次写入都同步到磁盘
- **自动恢复**: 启动时自动重放 WAL
- **空间管理**: flush 后自动清理旧 WAL

## 3. Compaction 机制

### 设计理念
参考 LevelDB/RocksDB 的分层 Compaction 策略，将多个小 SSTable 合并为更大的文件，减少文件数量，提高查询效率。

### 分层架构
```
Level 0: 4-8 个文件 (从 MemTable flush)
Level 1: 约 10 倍 Level 0
Level 2: 约 10 倍 Level 1
...
Level 7: 最大层级
```

### Compactor 类

#### 核心参数
- `MAX_LEVEL = 7`: 最大层级
- `LEVEL0_COMPACTION_TRIGGER = 4`: Level 0 触发阈值
- `SIZE_RATIO = 10`: 层级间大小比例

#### Compaction 策略

1. **Level 0 Compaction**
   - 触发条件: 文件数 >= 4
   - 选择文件: 所有 Level 0 文件
   - 目标层级: Level 1

2. **Level N Compaction (N >= 1)**
   - 触发条件: `size(Level N) / size(Level N+1) > 10`
   - 选择文件: 重叠区域的文件
   - 目标层级: Level N+1

### 工作流程

```
1. 检查是否需要 Compaction
   ├─ needsCompaction()
   └─ 检查各层级状态

2. 选择 Compaction 层级
   ├─ Level 0 优先
   └─ 其他层级按大小比例

3. 选择参与文件
   ├─ Level 0: 所有文件
   └─ Level N: 重叠文件

4. 执行合并
   ├─ 读取所有输入文件
   ├─ 按键排序去重
   └─ 写入目标层级

5. 更新元数据
   ├─ 删除输入文件
   └─ 添加输出文件
```

### LeveledSSTable 结构
```java
class LeveledSSTable {
    String path;       // 文件路径
    int level;         // 所在层级
    RowKey minKey;     // 最小键
    RowKey maxKey;     // 最大键
    long size;         // 文件大小
    long rowCount;     // 行数
}
```

## 集成示例

### 完整写入流程
```
用户调用 put(row)
    ↓
1. WAL.append(row)  ← 持久化保证
    ↓
2. MemTable.put(row)
    ↓
3. 检查 MemTable 是否满
    ↓ (满了)
4. Flush MemTable → SSTable (Level 0)
    ↓
5. 切换新 WAL
    ↓
6. 检查是否需要 Compaction
    ↓ (需要)
7. 执行 Compaction
    ↓
8. 删除旧文件
```

### 完整读取流程
```
用户调用 get(key)
    ↓
1. 查询 Active MemTable
    ↓ (未找到)
2. 查询 Immutable MemTable
    ↓ (未找到)
3. 按层级查询 SSTables
   ├─ Level 0
   ├─ Level 1
   └─ ...
    ↓
返回结果
```

## 性能优化

### WAL 优化
- **批量写入**: 未来可支持批量提交
- **异步刷盘**: 可配置异步 fsync

### Compaction 优化
- **并发 Compaction**: 不同层级可并行
- **增量 Compaction**: 避免一次处理过多数据
- **范围过滤**: 使用 min/max key 快速跳过文件

## 参考资料

- [Apache Paimon](https://paimon.apache.org/)
- [LevelDB Design](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB Compaction](https://github.com/facebook/rocksdb/wiki/Compaction)

## 运行示例

```bash
# 编译项目
mvn clean compile

# 运行高级特性示例
mvn exec:java -Dexec.mainClass="com.minipaimon.storage.AdvancedFeaturesExample"
```
