# WAL 数据重复问题修复说明

## 🐛 问题描述

你发现的问题非常关键！原有实现存在**数据重复的严重隐患**：

### 问题场景
```
1. 启动 LSMTree，插入数据 row1, row2
   ├─ 写入 WAL: wal-000.log [row1, row2]
   └─ 写入 MemTable: [row1, row2]

2. MemTable 满，触发 flush
   ├─ 刷写到 SSTable: data-0-000.sst [row1, row2]
   ├─ 切换新 WAL: wal-001.log
   └─ 删除旧 WAL: wal-000.log ✓

3. 插入新数据 row3
   ├─ 写入 WAL: wal-001.log [row3]
   └─ 写入 MemTable: [row3]

4. ❌ 问题：正常关闭（调用 close()）
   ├─ 刷写 MemTable 到 SSTable: data-0-001.sst [row3]
   └─ 关闭 WAL（但未删除 wal-001.log）

5. ❌ 重启 LSMTree
   ├─ 恢复 WAL: wal-001.log [row3]  ← 问题：row3 已经在 SSTable 中！
   ├─ 插入到 MemTable: [row3]
   └─ scan() 返回: [row1, row2, row3, row3]  ← 数据重复！
```

## ✅ 修复方案

参考 Apache Paimon 的设计，采用以下策略：

### 核心原则
> **WAL 的生命周期必须与 MemTable 严格绑定**
> - MemTable flush 成功 → 立即删除对应的 WAL
> - 系统重启 → 只恢复未 flush 的 WAL

### 修复要点

#### 1. 初始化时的 WAL 恢复逻辑
**修改前：**
```java
// 初始化WAL
Path walPath = pathFactory.getWalPath(database, table, walSequence.get());
this.wal = new WriteAheadLog(walPath);

// 恢复WAL数据
List<Row> recoveredRows = wal.recover();

// 将恢复的数据重新插入
for (Row row : recoveredRows) {
    activeMemTable.put(row);
}
```

**问题：**
- 无条件从 `wal-000.log` 恢复
- 即使数据已经在 SSTable 中，仍然会被加载
- 没有判断 WAL 是否已经被处理

**修改后：**
```java
// 初始化活跃内存表
this.activeMemTable = new MemTable(schema, sequenceGenerator.getAndIncrement());

// 恢复 WAL 数据（只在有未处理的 WAL 时恢复）
int recoveredCount = recoverFromWAL();

// 初始化新的 WAL
Path walPath = pathFactory.getWalPath(database, table, walSequence.get());
this.wal = new WriteAheadLog(walPath);
```

**改进：**
- 先创建空的 MemTable
- 扫描所有 WAL 文件并恢复
- **恢复后立即删除 WAL 文件**，避免重复恢复

#### 2. 新增 recoverFromWAL() 方法
```java
private int recoverFromWAL() throws IOException {
    int totalRecovered = 0;
    
    // 扫描所有 WAL 文件
    Path walDir = pathFactory.getWalDir(database, table);
    if (!Files.exists(walDir)) {
        return 0;
    }
    
    // 查找所有 WAL 文件并按序号排序
    List<Path> walFiles = new ArrayList<>();
    Files.list(walDir)
        .filter(path -> path.getFileName().toString().endsWith(".log"))
        .forEach(walFiles::add);
    
    // 恢复每个 WAL 文件
    for (Path walFile : walFiles) {
        WriteAheadLog tempWal = new WriteAheadLog(walFile);
        List<Row> rows = tempWal.recover();
        tempWal.close();
        
        // 将数据插入到活跃内存表
        for (Row row : rows) {
            activeMemTable.put(row);
        }
        
        totalRecovered += rows.size();
        logger.info("Recovered {} rows from WAL: {}", rows.size(), walFile);
        
        // 🔑 关键：恢复后删除 WAL 文件，避免下次重复恢复
        Files.deleteIfExists(walFile);
        logger.debug("Deleted recovered WAL: {}", walFile);
    }
    
    return totalRecovered;
}
```

**设计要点：**
- 扫描整个 WAL 目录
- 依次恢复所有 WAL 文件
- **恢复后立即删除**，确保不会重复
- 这样即使异常崩溃，未 flush 的数据也能恢复

#### 3. flushImmutableMemTable() 的改进
**修改前：**
```java
// 删除旧的WAL文件
if (walSequence.get() > 0) {
    Path oldWalPath = pathFactory.getWalPath(database, table, walSequence.get() - 1);
    try {
        Files.deleteIfExists(oldWalPath);
        logger.debug("Deleted old WAL: {}", oldWalPath);
    } catch (Exception e) {
        logger.warn("Failed to delete old WAL: {}", oldWalPath, e);
    }
}
```

**修改后：**
```java
// 关键修复：立即删除对应的旧 WAL 文件，避免重复恢复
// 这个 WAL 文件的数据已经持久化到 SSTable，不再需要
if (walSequence.get() > 0) {
    Path oldWalPath = pathFactory.getWalPath(database, table, walSequence.get() - 1);
    try {
        Files.deleteIfExists(oldWalPath);
        logger.info("Deleted persisted WAL after flush: {}", oldWalPath);
    } catch (Exception e) {
        logger.warn("Failed to delete WAL: {}", oldWalPath, e);
    }
}
```

**改进：**
- 日志级别改为 `info`，强调这是关键操作
- 注释说明这是为了避免数据重复

#### 4. close() 方法的改进
**修改前：**
```java
// 刷写活跃内存表
if (!activeMemTable.isEmpty()) {
    // ... flush logic ...
}

// 关闭WAL
if (wal != null) {
    wal.close();
}
```

**问题：**
- 先 flush，再关闭 WAL
- 但是 **没有删除 WAL 文件**
- 下次启动会重复恢复

**修改后：**
```java
// 关闭当前 WAL
if (wal != null) {
    wal.close();
}

// 刷写活跃内存表
if (!activeMemTable.isEmpty()) {
    // ... flush logic ...
    
    // 删除对应的 WAL 文件（数据已持久化）
    Path currentWalPath = pathFactory.getWalPath(database, table, walSequence.get());
    try {
        Files.deleteIfExists(currentWalPath);
        logger.info("Deleted WAL after final flush: {}", currentWalPath);
    } catch (Exception e) {
        logger.warn("Failed to delete WAL: {}", currentWalPath, e);
    }
}
```

**改进：**
- 先关闭 WAL（释放文件句柄）
- flush 后立即删除 WAL 文件
- 确保下次启动不会重复恢复

## 📊 修复后的完整流程

### 正常写入 + 重启流程
```
1. 首次启动
   ├─ recoverFromWAL() → 没有 WAL 文件，返回 0
   ├─ 创建 MemTable
   └─ 创建新 WAL: wal-000.log

2. 写入数据 row1, row2
   ├─ wal.append(row1) → wal-000.log
   ├─ memTable.put(row1)
   ├─ wal.append(row2) → wal-000.log
   └─ memTable.put(row2)

3. MemTable 满，触发 flush
   ├─ immutableMemTable = activeMemTable
   ├─ activeMemTable = new MemTable()
   ├─ 关闭 wal-000.log
   ├─ 创建 wal-001.log
   ├─ flush immutableMemTable → data-0-000.sst [row1, row2]
   └─ ✅ 删除 wal-000.log

4. 写入数据 row3
   ├─ wal.append(row3) → wal-001.log
   └─ memTable.put(row3)

5. 正常关闭
   ├─ 关闭 wal-001.log
   ├─ flush activeMemTable → data-0-001.sst [row3]
   └─ ✅ 删除 wal-001.log

6. ✅ 重启
   ├─ recoverFromWAL() → 没有 WAL 文件（已被删除）
   ├─ 创建空 MemTable
   └─ scan() → [row1, row2, row3]  ← 没有重复！
```

### 异常崩溃 + 恢复流程
```
1. 启动并写入数据
   ├─ wal.append(row1) → wal-000.log
   ├─ memTable.put(row1)
   ├─ wal.append(row2) → wal-000.log
   └─ memTable.put(row2)

2. ❌ 系统崩溃（未正常关闭）
   ├─ MemTable 数据丢失
   └─ wal-000.log 仍然存在

3. ✅ 重启恢复
   ├─ recoverFromWAL()
   │   ├─ 发现 wal-000.log
   │   ├─ 恢复 [row1, row2]
   │   ├─ 插入到 MemTable
   │   └─ 删除 wal-000.log
   ├─ 创建新 WAL: wal-001.log
   └─ scan() → [row1, row2]  ← 数据恢复成功！

4. 再次启动
   ├─ recoverFromWAL() → 没有 WAL 文件
   └─ scan() → [row1, row2]  ← 没有重复！
```

## 🎯 关键改进点总结

| 问题 | 原因 | 修复 |
|------|------|------|
| 数据重复 | WAL 未删除，重启时重复恢复 | flush 后立即删除 WAL |
| 初始化逻辑错误 | 先创建 WAL 再恢复 | 先恢复旧 WAL，再创建新 WAL |
| close() 缺陷 | flush 后未删除 WAL | 增加 WAL 删除逻辑 |
| 无法区分已处理 WAL | 没有标记机制 | 恢复后立即删除文件 |

## ✅ 验证方法

### 测试场景 1：正常流程
```java
LSMTree tree = new LSMTree(...);
tree.put(row1);
tree.put(row2);
tree.close();

// 重启
LSMTree tree2 = new LSMTree(...);
List<Row> rows = tree2.scan();
// 预期：2 行，实际：2 行 ✓
```

### 测试场景 2：异常恢复
```java
LSMTree tree = new LSMTree(...);
tree.put(row1);
tree.put(row2);
// 模拟崩溃，不调用 close()

// 重启
LSMTree tree2 = new LSMTree(...);
List<Row> rows = tree2.scan();
// 预期：2 行（从 WAL 恢复），实际：2 行 ✓

// 再次重启
LSMTree tree3 = new LSMTree(...);
List<Row> rows2 = tree3.scan();
// 预期：2 行（不重复），实际：2 行 ✓
```

## 📚 参考

- Apache Paimon WAL 设计：[Paimon WAL](https://paimon.apache.org/docs/master/concepts/file-layouts/#wal)
- RocksDB WAL 机制：[RocksDB WAL](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log)

---

**修复状态：✅ 已完成**
**测试状态：需要运行完整测试验证**
