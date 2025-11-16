# Paimon Commit 两阶段流程与 MVCC 详细分析

## 1. 核心概念

### 1.1 Snapshot（快照）
- **作用**：每次提交生成一个快照，表示表的一个不可变版本
- **ID 管理**：从 1 开始连续递增的快照 ID
- **存储位置**：`warehouse/db_name/table_name/snapshot/snapshot-{id}`

**快照核心字段**：
```java
- id: 快照 ID（版本号）
- schemaId: 对应的 schema 版本
- baseManifestList: 基础 manifest 列表（包含所有历史变更）
- deltaManifestList: 增量 manifest 列表（当前快照的新变更）
- changelogManifestList: changelog 列表（可选）
- commitUser: 提交用户标识
- commitIdentifier: 提交标识符（用于去重）
- commitKind: 提交类型（APPEND/COMPACT/OVERWRITE）
- totalRecordCount: 总记录数
```

### 1.2 Manifest 文件系统

```
Snapshot (快照)
    ↓
ManifestList (清单列表 - 文件路径列表)
    ↓
ManifestFile (清单文件 - 包含多个 ManifestEntry)
    ↓
ManifestEntry (清单条目 - 记录单个数据文件的变更)
    ↓
DataFile (实际的数据文件 - Parquet/ORC)
```

**ManifestEntry 结构**：
- `FileKind`: ADD 或 DELETE（逻辑删除）
- `partition`: 分区信息
- `bucket`: 桶号
- `file`: 数据文件元信息（文件名、大小、记录数、统计信息等）

### 1.3 关键组件

| 组件 | 作用 |
|------|------|
| `FileStoreCommitImpl` | 提交的核心实现，管理整个提交流程 |
| `SnapshotManager` | 快照管理器，负责快照的读取和查找 |
| `SnapshotCommit` | 原子提交接口（文件系统 rename 或外部锁） |
| `ManifestFile` | Manifest 文件的读写 |
| `ManifestList` | Manifest 列表的读写 |

---

## 2. Commit 两阶段流程详解

### 2.1 整体流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                     Phase 1: Prepare（准备阶段）                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 1. 收集 CommitMessage（来自各个 Writer）      │
    │    - 新增的数据文件                          │
    │    - 删除的数据文件                          │
    │    - Compaction 前后的文件                   │
    │    - Changelog 文件                          │
    └──────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 2. 分类收集变更                               │
    │    - appendTableFiles: 增量数据文件          │
    │    - appendChangelog: 增量 changelog         │
    │    - compactTableFiles: 合并后的文件         │
    │    - compactChangelog: 合并 changelog        │
    └──────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 3. 冲突预检查（Optimization）                 │
    │    - 读取最新 snapshot                       │
    │    - 读取变更分区的所有现有文件               │
    │    - 检查是否有文件冲突                       │
    └──────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Phase 2: Commit（提交阶段）                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 4. 准备新的 Snapshot                          │
    │    - 计算新的 snapshot ID = latest + 1       │
    │    - 合并旧的 manifest 文件（可选）          │
    │    - 写入新的 manifest 文件                  │
    │    - 写入 manifest list                      │
    └──────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 5. 冲突检查（如果 snapshot 变化）             │
    │    - 重新读取最新 snapshot                   │
    │    - 读取增量变更                            │
    │    - 检查文件删除冲突                        │
    │    - 检查 LSM 键范围冲突                     │
    └──────────────────────────────────────────────┘
                              │
                              ▼
    ┌──────────────────────────────────────────────┐
    │ 6. 原子提交 Snapshot                          │
    │    - 写入 snapshot-{id} 文件                 │
    │    - 使用文件系统 rename (HDFS)              │
    │    - 或使用外部锁 (S3/OSS)                   │
    └──────────────────────────────────────────────┘
                              │
                              ▼
            ┌─────────────────────────────────┐
            │   Commit 成功                   │
            └─────────────────────────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            │                                   │
            ▼                                   ▼
    ┌──────────────┐                  ┌──────────────┐
    │   成功返回    │                  │   失败重试    │
    └──────────────┘                  └──────────────┘
```

### 2.2 核心代码流程

#### 2.2.1 主流程入口

```java
// FileStoreCommitImpl.commit()
public int commit(ManifestCommittable committable, boolean checkAppendFiles) {
    // 分类收集变更
    collectChanges(committable.fileCommittables(), 
                   appendTableFiles, appendChangelog, 
                   compactTableFiles, compactChangelog,
                   appendHashIndexFiles, compactDvIndexFiles);
    
    // 第一次提交：增量数据
    if (!appendTableFiles.isEmpty() || !appendChangelog.isEmpty()) {
        // 冲突预检查
        if (latestSnapshot != null && checkAppendFiles) {
            baseEntries.addAll(readAllEntriesFromChangedPartitions(...));
            noConflictsOrFail(...);  // 提前检查冲突
            safeLatestSnapshotId = latestSnapshot.id();
        }
        
        // 提交（带重试）
        attempts += tryCommit(appendTableFiles, appendChangelog, ...);
        generatedSnapshot += 1;
    }
    
    // 第二次提交：合并数据（如果有）
    if (!compactTableFiles.isEmpty() || !compactChangelog.isEmpty()) {
        attempts += tryCommit(compactTableFiles, compactChangelog, ...);
        generatedSnapshot += 1;
    }
    
    return generatedSnapshot;
}
```

#### 2.2.2 尝试提交（带重试）

```java
private int tryCommit(...) {
    int retryCount = 0;
    long startMillis = System.currentTimeMillis();
    
    while (true) {
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        CommitResult result = tryCommitOnce(
            retryResult, tableFiles, changelogFiles, 
            indexFiles, identifier, watermark, 
            logOffsets, properties, commitKind, 
            latestSnapshot, conflictCheck, statsFileName
        );
        
        if (result.isSuccess()) {
            break;  // 成功
        }
        
        // 检查超时或重试次数
        if (System.currentTimeMillis() - startMillis > commitTimeout
                || retryCount >= commitMaxRetries) {
            throw new RuntimeException("Commit failed after retries");
        }
        
        commitRetryWait(retryCount);  // 指数退避
        retryCount++;
    }
    
    return retryCount + 1;
}
```

#### 2.2.3 单次提交尝试

```java
CommitResult tryCommitOnce(...) {
    // 1. 检查是否已提交（去重）
    if (retryResult != null && latestSnapshot != null) {
        for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
            Snapshot snapshot = snapshotManager.snapshot(i);
            if (snapshot.commitUser().equals(commitUser)
                    && snapshot.commitIdentifier() == identifier
                    && snapshot.commitKind() == commitKind) {
                return new SuccessResult();  // 已提交，直接返回
            }
        }
    }
    
    // 2. 计算新的 snapshot ID
    long newSnapshotId = latestSnapshot != null 
        ? latestSnapshot.id() + 1 
        : Snapshot.FIRST_SNAPSHOT_ID;
    
    // 3. 冲突检查
    if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
        baseDataFiles = readAllEntriesFromChangedPartitions(latestSnapshot, ...);
        noConflictsOrFail(latestSnapshot.commitUser(), 
                         baseDataFiles, 
                         SimpleFileEntry.from(deltaFiles), 
                         commitKind);
    }
    
    // 4. 准备 Snapshot
    try {
        // 合并旧的 manifest 文件
        mergeAfterManifests = ManifestFileMerger.merge(
            mergeBeforeManifests, manifestFile, 
            manifestTargetSize.getBytes(), ...);
        baseManifestList = manifestList.write(mergeAfterManifests);
        
        // 写入新的 manifest
        deltaManifestList = manifestList.write(manifestFile.write(deltaFiles));
        
        // 写入 changelog manifest
        if (!changelogFiles.isEmpty()) {
            changelogManifestList = manifestList.write(
                manifestFile.write(changelogFiles));
        }
        
        // 构建新的 Snapshot 对象
        newSnapshot = new Snapshot(
            newSnapshotId, schemaId,
            baseManifestList, deltaManifestList,
            changelogManifestList, indexManifest,
            commitUser, identifier, commitKind,
            System.currentTimeMillis(), ...);
    } catch (Throwable e) {
        // 失败清理
        cleanUpReuseTmpManifests(...);
        cleanUpNoReuseTmpManifests(...);
        throw new RuntimeException("Exception during prepare", e);
    }
    
    // 5. 原子提交
    boolean success;
    try {
        success = commitSnapshotImpl(newSnapshot, deltaStatistics);
    } catch (Exception e) {
        return new RetryResult(latestSnapshot, baseDataFiles, e);
    }
    
    if (!success) {
        // 提交失败，清理并重试
        cleanUpNoReuseTmpManifests(...);
        return new RetryResult(latestSnapshot, baseDataFiles, null);
    }
    
    // 6. 成功，执行回调
    commitCallbacks.forEach(callback -> 
        callback.call(baseDataFiles, deltaFiles, indexFiles, newSnapshot));
    return new SuccessResult();
}
```

#### 2.2.4 原子提交实现

```java
private boolean commitSnapshotImpl(Snapshot newSnapshot, 
                                   List<PartitionEntry> deltaStatistics) {
    try {
        // 委托给 SnapshotCommit 接口
        // - 对于 HDFS：使用文件系统的原子 rename
        // - 对于 S3/OSS：需要外部锁（Hive Metastore / JDBC）
        return snapshotCommit.commit(newSnapshot, branchName, statistics);
    } catch (Throwable e) {
        throw new RuntimeException(
            "Exception during atomic commit, cannot clean up", e);
    }
}
```

---

## 3. MVCC 并发控制机制

### 3.1 MVCC 核心原理

Paimon 使用 **乐观并发控制（Optimistic Concurrency Control）**：

```
Writer A                Writer B
   │                       │
   ├─ Read Snapshot N      ├─ Read Snapshot N
   │                       │
   ├─ Write Data          ├─ Write Data
   │                       │
   ├─ Prepare Commit       │
   │  (Snapshot N+1)       │
   │                       ├─ Prepare Commit
   ├─ Atomic Write         │  (Snapshot N+1)
   │  snapshot-{N+1} ✓    │
   │                       ├─ Atomic Write
   │                       │  snapshot-{N+1} ✗ (冲突)
   │                       │
   │                       ├─ Retry from Snapshot N+1
   │                       │
   │                       ├─ Prepare Commit
   │                       │  (Snapshot N+2)
   │                       │
   │                       ├─ Atomic Write
   │                       │  snapshot-{N+2} ✓
```

### 3.2 并发冲突类型

#### 3.2.1 Snapshot ID 冲突

**场景**：多个 writer 同时尝试写入同一个 snapshot ID

**机制**：
- Snapshot ID 必须唯一且连续
- 使用文件系统的原子操作确保只有一个成功
- 失败的 writer 读取最新 snapshot 并重试

**代码体现**：
```java
// 计算新的 snapshot ID
long newSnapshotId = latestSnapshot.id() + 1;

// 原子写入 snapshot 文件
// HDFS: rename 是原子的
// S3/OSS: 需要外部锁
boolean success = snapshotCommit.commit(newSnapshot, ...);
if (!success) {
    // Snapshot ID 被抢占，需要重试
    return new RetryResult(...);
}
```

#### 3.2.2 文件删除冲突

**场景**：多个 writer 尝试删除同一个文件

**机制**：
- 在提交前检查要删除的文件是否还存在
- 使用 `FileEntry.mergeEntries()` 合并变更并检测冲突
- 如果文件已被删除，提交失败并重启

**代码体现**：
```java
private void noConflictsOrFail(...) {
    List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
    allEntries.addAll(changes);
    
    // 合并所有变更
    Collection<SimpleFileEntry> mergedEntries;
    try {
        mergedEntries = FileEntry.mergeEntries(allEntries);
    } catch (Throwable e) {
        throw exceptionFunction.apply(e);
    }
    
    // 检查是否有文件被意外删除
    for (SimpleFileEntry entry : mergedEntries) {
        Preconditions.checkState(
            entry.kind() != FileKind.DELETE,
            "Trying to delete file %s which is not previously added.",
            entry.fileName());
    }
}
```

**FileEntry.mergeEntries 逻辑**：
```java
static void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
    for (T entry : entries) {
        Identifier identifier = entry.identifier();
        switch (entry.kind()) {
            case ADD:
                // 文件必须是新增的
                Preconditions.checkState(!map.containsKey(identifier),
                    "Trying to add file %s which is already added.");
                map.put(identifier, entry);
                break;
            case DELETE:
                if (map.containsKey(identifier)) {
                    // ADD 和 DELETE 抵消
                    map.remove(identifier);
                } else {
                    // 保留 DELETE 标记（文件在之前的 manifest 中）
                    map.put(identifier, entry);
                }
                break;
        }
    }
}
```

#### 3.2.3 LSM 键范围冲突

**场景**：主键表中，新写入的文件与现有文件的键范围重叠

**机制**：
- 对于 Level >= 1 的文件，键范围不能重叠
- 在提交前检查键范围
- 如果有重叠，提交失败

**代码体现**：
```java
// 按 partition, bucket, level 分组
Map<LevelIdentifier, List<SimpleFileEntry>> levels = ...;

// 检查每个 level 的文件键范围
for (List<SimpleFileEntry> entries : levels.values()) {
    // 按 minKey 排序
    entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
    
    // 检查相邻文件的键范围是否重叠
    for (int i = 0; i + 1 < entries.size(); i++) {
        SimpleFileEntry a = entries.get(i);
        SimpleFileEntry b = entries.get(i + 1);
        if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
            // 键范围冲突
            throw createConflictException("LSM conflicts detected!");
        }
    }
}
```

### 3.3 重试机制

**指数退避策略**：
```java
private void commitRetryWait(int retryCount) {
    // 指数退避：min(baseWait * 2^retryCount, maxWait)
    int retryWait = (int) Math.min(
        commitMinRetryWait * Math.pow(2, retryCount), 
        commitMaxRetryWait);
    
    // 添加随机抖动（±20%）
    ThreadLocalRandom random = ThreadLocalRandom.current();
    retryWait += random.nextInt(Math.max(1, (int) (retryWait * 0.2)));
    
    TimeUnit.MILLISECONDS.sleep(retryWait);
}
```

**配置参数**：
- `commit-timeout`: 提交超时时间（默认 10 分钟）
- `commit-max-retries`: 最大重试次数（默认 Integer.MAX_VALUE）
- `commit-retry-min-wait`: 最小等待时间（默认 100ms）
- `commit-retry-max-wait`: 最大等待时间（默认 30s）

### 3.4 去重机制

**场景**：重试时避免重复提交

**机制**：
- 使用 `(commitUser, commitIdentifier, commitKind)` 三元组标识一次提交
- 在重试时，检查最新的 snapshot 是否已包含该提交
- 如果已提交，直接返回成功

**代码体现**：
```java
// 检查是否已提交
for (long i = startCheckSnapshot; i <= latestSnapshot.id(); i++) {
    Snapshot snapshot = snapshotManager.snapshot(i);
    if (snapshot.commitUser().equals(commitUser)
            && snapshot.commitIdentifier() == identifier
            && snapshot.commitKind() == commitKind) {
        return new SuccessResult();  // 已提交，跳过
    }
}
```

---

## 4. 多 Writer 并发写入

### 4.1 并发写入模式

#### 模式 1：不同分区并发写入（推荐）
```
Writer A → Partition 2024-01-01
Writer B → Partition 2024-01-02
```
- **冲突概率**：低
- **性能**：高
- **适用场景**：流式写入最新分区 + 批处理回填历史分区

#### 模式 2：相同分区并发写入（需要专用 Compaction）
```
Writer A ─┐
Writer B ─┼─→ Partition 2024-01-01
Writer C ─┘
```
- **问题**：Compaction 会产生文件删除冲突
- **解决方案**：设置 `write-only=true`，使用专用 Compaction 作业
- **适用场景**：多流式作业写入同一分区

### 4.2 专用 Compaction 架构

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Writer A   │      │  Writer B   │      │  Writer C   │
│ (write-only)│      │ (write-only)│      │ (write-only)│
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │  Paimon Table    │
                  └──────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │ Compaction Job   │
                  │  (专用合并任务)   │
                  └──────────────────┘
```

**配置**：
```sql
-- Writer 配置
CREATE TABLE t (...) WITH (
    'write-only' = 'true',  -- 禁用自动 compaction
    ...
);

-- Compaction 作业
INSERT INTO t /*+ OPTIONS('sink.parallelism'='1') */ 
SELECT * FROM t /*+ OPTIONS('scan.mode'='compact') */;
```

---

## 5. Snapshot 隔离级别

### 5.1 快照隔离（Snapshot Isolation）

**特性**：
- 读取总是看到一致的快照
- 写入可能会被混合（对于相同分区的并发写入）
- 不会丢失数据

**示例**：
```
Time →

t1: Writer A 读取 Snapshot 10
t2: Writer B 读取 Snapshot 10
t3: Writer A 写入文件 F1, F2 → Snapshot 11
t4: Writer B 写入文件 F3, F4 → Snapshot 12

最终状态：Snapshot 12 包含 F1, F2, F3, F4
```

### 5.2 可见性规则

| 操作 | 可见性 |
|------|--------|
| 写入 snapshot 文件后 | 立即对所有读取者可见 |
| 未写入 snapshot | 不可见（即使数据文件已存在） |
| 删除的 snapshot | 不可见（但文件可能还在，等待清理） |

---

## 6. 文件系统原子性

### 6.1 HDFS（原子 Rename）

```java
// FileSystemCatalogFactory.SnapshotCommitImpl
public boolean commit(Snapshot snapshot, ...) {
    Path tmpPath = snapshotManager.snapshotPath(snapshot.id() + "-tmp");
    Path targetPath = snapshotManager.snapshotPath(snapshot.id());
    
    // 1. 写入临时文件
    fileIO.writeFileUtf8(tmpPath, snapshot.toJson());
    
    // 2. 原子 rename（HDFS 保证原子性）
    try {
        if (!fileIO.exists(targetPath)) {
            fileIO.rename(tmpPath, targetPath);
            return true;
        } else {
            // Snapshot ID 被抢占
            fileIO.delete(tmpPath);
            return false;
        }
    } catch (Exception e) {
        return false;
    }
}
```

### 6.2 S3/OSS（需要外部锁）

**问题**：S3/OSS 的 rename 不是原子的

**解决方案**：使用外部锁
```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://bucket/warehouse',
    'lock.enabled' = 'true',
    'metastore' = 'hive',  -- 或 'jdbc'
    'uri' = 'thrift://localhost:9083'
);
```

**锁机制**：
```java
// 使用 Hive Metastore 或 JDBC 锁
Lock lock = catalogLockFactory.create();
try {
    lock.lock();  // 获取锁
    
    // 提交 snapshot
    if (!fileIO.exists(targetPath)) {
        fileIO.rename(tmpPath, targetPath);
        success = true;
    }
} finally {
    lock.unlock();  // 释放锁
}
```

---

## 7. Mini Paimon 实现建议

基于以上分析，实现一个简化版的 Mini Paimon 可以聚焦以下核心：

### 7.1 核心数据结构

```java
// 1. Snapshot（快照）
class Snapshot {
    long id;
    String manifestList;  // 简化：只保留一个 manifest list
    long timestamp;
    String commitUser;
    long commitId;
}

// 2. ManifestEntry（文件变更条目）
class ManifestEntry {
    enum Kind { ADD, DELETE }
    Kind kind;
    String fileName;
    long fileSize;
    long recordCount;
}

// 3. DataFile（数据文件元信息）
class DataFile {
    String fileName;
    long fileSize;
    long recordCount;
    Map<String, String> stats;  // 简化的统计信息
}
```

### 7.2 简化的提交流程

```java
class SimplifiedCommit {
    public boolean commit(List<ManifestEntry> changes) {
        // 1. 读取最新 snapshot
        Snapshot latest = snapshotManager.latestSnapshot();
        long newSnapshotId = (latest != null) ? latest.id + 1 : 1;
        
        // 2. 冲突检查（简化：只检查文件删除冲突）
        if (latest != null) {
            Set<String> existingFiles = readExistingFiles(latest);
            for (ManifestEntry entry : changes) {
                if (entry.kind == DELETE && !existingFiles.contains(entry.fileName)) {
                    throw new ConflictException("File already deleted");
                }
            }
        }
        
        // 3. 写入 manifest 文件
        String manifestFile = writeManifest(changes);
        
        // 4. 创建新 snapshot
        Snapshot newSnapshot = new Snapshot(
            newSnapshotId, manifestFile, 
            System.currentTimeMillis(), ...);
        
        // 5. 原子提交（使用文件 rename）
        return atomicCommit(newSnapshot);
    }
    
    private boolean atomicCommit(Snapshot snapshot) {
        Path tmpPath = getTmpPath(snapshot.id);
        Path targetPath = getSnapshotPath(snapshot.id);
        
        // 写入临时文件
        writeJson(tmpPath, snapshot);
        
        // 原子 rename
        if (!fileExists(targetPath)) {
            rename(tmpPath, targetPath);
            return true;
        } else {
            delete(tmpPath);
            return false;  // 冲突，需要重试
        }
    }
}
```

### 7.3 简化的并发控制

```java
class SimplifiedMVCC {
    public void commitWithRetry(List<ManifestEntry> changes) {
        int maxRetries = 10;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                if (commit(changes)) {
                    return;  // 成功
                }
            } catch (ConflictException e) {
                // 文件冲突，不能重试
                throw e;
            }
            
            // Snapshot ID 冲突，指数退避后重试
            sleep((int) Math.pow(2, retryCount) * 100);
            retryCount++;
        }
        
        throw new RuntimeException("Commit failed after retries");
    }
}
```

### 7.4 可以省略的部分

为了简化实现，可以省略：
- ✗ Manifest 合并（每次都重新写所有文件）
- ✗ Changelog 支持
- ✗ 索引文件（hash index, deletion vector）
- ✗ 统计信息收集
- ✗ Schema 版本管理
- ✗ 分支（branch）支持
- ✗ 外部锁（只支持 HDFS）

### 7.5 必须保留的核心

为了保证正确性，必须保留：
- ✓ Snapshot ID 连续性
- ✓ 文件删除冲突检查
- ✓ 原子提交（rename）
- ✓ 重试机制
- ✓ 去重检查

---

## 8. 总结

### 8.1 关键设计要点

| 设计点 | 作用 |
|--------|------|
| Snapshot ID 连续性 | 保证版本顺序，简化查找 |
| 乐观并发控制 | 提高并发性能 |
| 文件逻辑删除 | 避免误删，检测冲突 |
| 原子 Rename | 保证提交原子性 |
| 指数退避重试 | 避免活锁 |
| Manifest 分层 | 减少读取开销 |

### 8.2 并发写入的最佳实践

1. **不同分区写入**：自然避免冲突，性能最佳
2. **相同分区写入**：需要专用 Compaction 作业
3. **外部锁**：S3/OSS 必须配置
4. **重试策略**：设置合理的超时和重试次数
5. **监控**：关注冲突率和重试次数

### 8.3 MVCC 的优势

- **高并发**：读写互不阻塞
- **一致性**：每个快照都是一致的视图
- **隔离性**：避免脏读
- **可追溯**：可以回溯到任何历史版本

---

## 附录：重要源码文件

| 文件 | 作用 |
|------|------|
| `FileStoreCommitImpl.java` | 提交核心实现（核心） |
| `SnapshotManager.java` | 快照管理 |
| `Snapshot.java` | 快照数据结构 |
| `ManifestFile.java` | Manifest 文件读写 |
| `ManifestList.java` | Manifest 列表管理 |
| `FileEntry.java` | 文件变更合并逻辑 |
| `SnapshotCommit.java` | 原子提交接口 |
| `CatalogSnapshotCommit.java` | Catalog 级别提交 |

---

**文档生成时间**: 2025-11-16
**Paimon 版本**: Latest (基于 paimon-core)

