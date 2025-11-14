# Writer 架构重构说明

## 概述

参考开源 Apache Paimon 的设计,将写入逻辑重构为基于 `RecordWriter` 接口的架构,支持两种表类型:

1. **主键表 (Primary Key Table)** - 使用 `MergeTreeWriter`
2. **仅追加表 (Append-Only Table)** - 使用 `AppendOnlyWriter`

## 架构设计

### 1. RecordWriter 接口

```java
public interface RecordWriter extends AutoCloseable {
    void write(Row row) throws IOException;
    List<DataFileMeta> prepareCommit() throws IOException;
    void close() throws IOException;
}
```

定义了所有写入器的通用接口,支持:
- `write()` - 写入单行数据
- `prepareCommit()` - 准备提交,刷写数据并返回文件元信息
- `close()` - 关闭写入器

### 2. MergeTreeWriter - 主键表写入器

**特点:**
- 使用 LSM Tree 存储引擎
- 支持 UPDATE/DELETE (通过主键去重)
- 有 Merge Engine
- 需要 Compaction

**实现:**
- 内部使用 `LSMTree` 管理数据
- 写入时自动按主键去重
- 支持 MemTable、SSTable、Compaction 等完整的 LSM Tree 功能

**使用场景:**
- 需要更新/删除数据的表
- 需要保证主键唯一性的表
- OLTP 场景

### 3. AppendOnlyWriter - 仅追加表写入器

**特点:**
- 不使用 LSM Tree
- 直接写文件 (类似 CSV Writer)
- 只支持 INSERT
- 不需要 Compaction (可选)
- 性能更高 (没有合并开销)

**实现:**
- 使用 `MemTable` 作为缓冲区
- 达到阈值时直接刷写到 SSTable
- 不进行主键去重
- 不需要 Compaction

**使用场景:**
- 日志表、事件表
- 不需要更新/删除的表
- OLAP 场景
- 高吞吐写入场景

### 4. TableWrite - 统一写入入口

`TableWrite` 根据表的 Schema 自动选择合适的 Writer:

```java
if (tableType.isPrimaryKey()) {
    // 使用 MergeTreeWriter
    return new MergeTreeWriter(...);
} else {
    // 使用 AppendOnlyWriter
    return new AppendOnlyWriter(...);
}
```

## 表类型检测

通过 `TableType.fromSchema(schema)` 自动检测表类型:

```java
public enum TableType {
    APPEND_ONLY,    // 无主键
    PRIMARY_KEY;    // 有主键
    
    public static TableType fromSchema(Schema schema) {
        return schema.hasPrimaryKey() ? PRIMARY_KEY : APPEND_ONLY;
    }
}
```

## 使用示例

### 主键表 (自动使用 MergeTreeWriter)

```java
// 创建主键表
Schema schema = new Schema(0, fields, Arrays.asList("id"));
catalog.createTable(identifier, schema, false);
Table table = catalog.getTable(identifier);

// 写入数据
TableWrite tableWrite = table.newWrite();
tableWrite.write(new Row(new Object[]{1, "Alice"}));
tableWrite.write(new Row(new Object[]{1, "Alice Updated"})); // 更新

// 提交
TableWrite.TableCommitMessage msg = tableWrite.prepareCommit();
table.newCommit().commit(msg);
tableWrite.close();
```

### 仅追加表 (自动使用 AppendOnlyWriter)

```java
// 创建仅追加表 (无主键)
Schema schema = new Schema(0, fields);
catalog.createTable(identifier, schema, false);
Table table = catalog.getTable(identifier);

// 写入数据
TableWrite tableWrite = table.newWrite();
tableWrite.write(new Row(new Object[]{1, "Event1"}));
tableWrite.write(new Row(new Object[]{1, "Event2"})); // 允许重复

// 提交
TableWrite.TableCommitMessage msg = tableWrite.prepareCommit();
table.newCommit().commit(msg);
tableWrite.close();
```

## 性能对比

根据测试结果:
- **MergeTreeWriter** (主键表): 写入1000行耗时 ~170ms
- **AppendOnlyWriter** (仅追加表): 写入1000行耗时 ~70ms

仅追加表的写入性能约为主键表的 **2.4倍**,因为:
1. 不需要主键去重
2. 不需要 LSM Tree 的复杂操作
3. 不需要 Compaction

## 代码结构

```
paimon/src/main/java/com/mini/paimon/
├── storage/
│   ├── RecordWriter.java          # 写入器接口
│   ├── MergeTreeWriter.java       # 主键表写入器
│   ├── AppendOnlyWriter.java      # 仅追加表写入器
│   ├── LSMTree.java                # LSM Tree 引擎
│   ├── MemTable.java               # 内存表
│   ├── SSTableWriter.java          # SSTable 写入器
│   └── ...
├── table/
│   ├── TableWrite.java             # 统一写入入口
│   └── ...
└── metadata/
    ├── TableType.java              # 表类型枚举
    └── ...
```

## 测试验证

所有现有测试均通过 (105个测试用例):

```bash
mvn clean test
```

测试结果:
```
[INFO] Tests run: 105, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

## 参考

- Apache Paimon: https://github.com/apache/paimon
- Paimon MergeTreeWriter: `org.apache.paimon.table.sink.MergeTreeWriter`
- Paimon AppendOnlyWriter: `org.apache.paimon.table.sink.AppendOnlyWriter`

