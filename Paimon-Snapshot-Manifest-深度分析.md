# Paimon Snapshot 和 Manifest 深度实现分析

> 本文档基于 Apache Paimon 源码，深入分析 Snapshot 和 Manifest 的实现细节，旨在为实现 mini 版本的 Paimon 提供参考。

## 目录

- [1. 整体架构概览](#1-整体架构概览)
- [2. Snapshot 详细实现](#2-snapshot-详细实现)
- [3. Manifest 体系详解](#3-manifest-体系详解)
- [4. 写入和提交流程](#4-写入和提交流程)
- [5. 读取流程分析](#5-读取流程分析)
- [6. 文件组织结构](#6-文件组织结构)
- [7. 关键设计要点](#7-关键设计要点)
- [8. Mini 版本实现建议](#8-mini-版本实现建议)

---

## 1. 整体架构概览

### 1.1 核心设计理念

Paimon 采用了**类似 Iceberg 的 Snapshot + Manifest 架构**，实现了：
- **MVCC（多版本并发控制）**：每次提交生成新的 Snapshot
- **增量更新**：通过 Delta Manifest 加速增量读取
- **时间旅行**：可以读取任意历史 Snapshot
- **高效的元数据管理**：分层的 Manifest 设计减少元数据开销

### 1.2 四层元数据架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Snapshot                             │
│  (快照文件，JSON格式，记录表的某个时刻的完整状态)                │
└──────────────────┬──────────────────────────────────────────┘
                   │
          ┌────────┴────────┐
          │                  │
┌─────────▼────────┐  ┌─────▼──────────┐
│  Base Manifest   │  │ Delta Manifest  │
│      List        │  │      List       │
│ (记录所有文件)    │  │  (记录增量)     │
└────────┬─────────┘  └────────┬────────┘
         │                     │
         └─────────┬───────────┘
                   │
         ┌─────────▼──────────┐
         │   Manifest File    │
         │  (存储文件元数据)   │
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │  Manifest Entry    │
         │  (单个文件的变更)   │
         └─────────┬──────────┘
                   │
         ┌─────────▼──────────┐
         │   Data File Meta   │
         │  (数据文件的元数据)  │
         └────────────────────┘
```

### 1.3 关键组件说明

| 组件 | 文件格式 | 作用 | 位置 |
|------|---------|------|------|
| Snapshot | JSON | 记录表在某个时刻的完整状态 | `{table_path}/snapshot/snapshot-{id}` |
| ManifestList | Avro/Parquet | 记录所有 ManifestFile 的元信息 | `{table_path}/manifest/manifest-list-{uuid}` |
| ManifestFile | Avro/Parquet | 记录多个 ManifestEntry | `{table_path}/manifest/manifest-{uuid}` |
| ManifestEntry | - | 单个数据文件的增删信息 | 存储在 ManifestFile 中 |
| DataFile | Avro/ORC/Parquet | 实际的数据文件 | `{table_path}/bucket-{n}/data-{uuid}.{ext}` |

---

## 2. Snapshot 详细实现

### 2.1 Snapshot 数据结构

Snapshot 是 Paimon 中最核心的元数据对象，每次提交都会生成一个新的 Snapshot。

#### 源码定义（Snapshot.java）

```java
public class Snapshot implements Serializable {
    // 版本号（当前为 3）
    protected final Integer version;
    
    // Snapshot ID（从 1 开始递增）
    protected final long id;
    
    // Schema ID（关联的表结构版本）
    protected final long schemaId;
    
    // Base Manifest List（记录所有文件的 manifest list）
    protected final String baseManifestList;
    protected final Long baseManifestListSize;
    
    // Delta Manifest List（记录本次变更的 manifest list）
    protected final String deltaManifestList;
    protected final Long deltaManifestListSize;
    
    // Changelog Manifest List（记录 changelog 的 manifest list）
    @Nullable protected final String changelogManifestList;
    @Nullable protected final Long changelogManifestListSize;
    
    // Index Manifest（索引文件）
    @Nullable protected final String indexManifest;
    
    // 提交信息
    protected final String commitUser;
    protected final long commitIdentifier;
    protected final CommitKind commitKind;  // APPEND/COMPACT/OVERWRITE/ANALYZE
    protected final long timeMillis;
    
    // 统计信息
    @Nullable protected final Long totalRecordCount;
    @Nullable protected final Long deltaRecordCount;
    @Nullable protected final Long changelogRecordCount;
    
    // 其他元数据
    @Nullable protected final Long watermark;
    @Nullable protected final String statistics;
    @Nullable protected final Map<String, String> properties;
    @Nullable protected final Long nextRowId;
}
```

### 2.2 Snapshot 的三种 Manifest List

#### 2.2.1 Base Manifest List
- **作用**：记录表的**完整状态**（所有有效的数据文件）
- **更新策略**：
  - 如果自上次生成 Base 以来，Delta 累积不多，直接复用上一个 Base
  - 如果 Delta 累积较多，将 Base + Delta 合并生成新的 Base
- **优化目的**：避免每次都扫描所有 Manifest 文件

#### 2.2.2 Delta Manifest List
- **作用**：记录**本次提交的增量变更**（新增/删除的文件）
- **用途**：
  - 流式读取：只读取增量数据
  - 快速过期：快速识别需要清理的文件
- **特点**：每次提交都会生成新的 Delta

#### 2.2.3 Changelog Manifest List
- **作用**：记录**用户级别的 changelog**
- **生成场景**：
  - 使用 CDC 模式写入时
  - Full-Compaction 时生成完整的 changelog
- **特点**：可选，不是所有提交都有 changelog

### 2.3 Snapshot 文件格式

Snapshot 使用 **JSON 格式**存储，便于人类阅读和调试。

#### 示例 JSON 文件

```json
{
  "version": 3,
  "id": 5,
  "schemaId": 0,
  "baseManifestList": "manifest-list-a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "baseManifestListSize": 1024,
  "deltaManifestList": "manifest-list-b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "deltaManifestListSize": 256,
  "changelogManifestList": null,
  "changelogManifestListSize": null,
  "indexManifest": "index-manifest-12345",
  "commitUser": "user1",
  "commitIdentifier": 1234567890,
  "commitKind": "APPEND",
  "timeMillis": 1701234567890,
  "logOffsets": {},
  "totalRecordCount": 10000,
  "deltaRecordCount": 100,
  "changelogRecordCount": null,
  "watermark": null,
  "statistics": "stats-file-xyz",
  "properties": {},
  "nextRowId": 10100
}
```

### 2.4 SnapshotManager 实现

#### 核心职责

`SnapshotManager` 负责 Snapshot 的管理，提供以下功能：

1. **路径管理**：生成 Snapshot 文件路径
2. **读写操作**：读取和缓存 Snapshot
3. **查询功能**：
   - 最新/最早 Snapshot
   - 根据时间戳查询 Snapshot
   - 根据 watermark 查询 Snapshot
4. **Hint 文件管理**：维护 latest/earliest hint 文件加速查询

#### 关键方法源码分析

```java
public class SnapshotManager implements Serializable {
    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;
    private final SnapshotLoader snapshotLoader;
    private final Cache<Path, Snapshot> cache;  // Caffeine 缓存
    
    // Snapshot 路径: {tablePath}/{branch}/snapshot/snapshot-{id}
    public Path snapshotPath(long snapshotId) {
        return new Path(
            branchPath(tablePath, branch) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId
        );
    }
    
    // 读取 Snapshot（带缓存）
    public Snapshot snapshot(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        Snapshot snapshot = cache == null ? null : cache.getIfPresent(path);
        if (snapshot == null) {
            snapshot = fromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
    }
    
    // 获取最新 Snapshot（优先使用 SnapshotLoader）
    public Snapshot latestSnapshot() {
        if (snapshotLoader != null) {
            try {
                return snapshotLoader.load().orElse(null);
            } catch (UnsupportedOperationException ignored) {
            }
        }
        return latestSnapshotFromFileSystem();
    }
    
    // 根据时间戳二分查找 Snapshot
    public Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long latest = latestSnapshotId();
        if (latest == null) return null;
        
        Snapshot earliestSnapshot = earliestSnapshot(latest);
        if (earliestSnapshot == null || 
            earliestSnapshot.timeMillis() > timestampMills) {
            return null;
        }
        
        long earliest = earliestSnapshot.id();
        Snapshot finalSnapshot = null;
        
        // 二分查找
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2;
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            
            if (commitTime > timestampMills) {
                latest = mid - 1;
            } else if (commitTime < timestampMills) {
                earliest = mid + 1;
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot;
                break;
            }
        }
        return finalSnapshot;
    }
}
```

#### Hint 文件机制

为了加速 `latest` 和 `earliest` Snapshot 的查找，Paimon 使用了 **Hint 文件**：

- **latest-hint**：记录最新的 Snapshot ID
- **earliest-hint**：记录最早的 Snapshot ID
- **作用**：避免每次都扫描整个目录
- **更新时机**：
  - latest hint：每次提交时更新
  - earliest hint：过期删除时更新

---

## 3. Manifest 体系详解

### 3.1 ManifestList 实现

`ManifestList` 管理多个 `ManifestFile` 的元信息。

#### 核心结构

```java
public class ManifestList extends ObjectsFile<ManifestFileMeta> {
    
    // 读取所有 Manifest（包括 data 和 changelog）
    public List<ManifestFileMeta> readAllManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(readDataManifests(snapshot));
        result.addAll(readChangelogManifests(snapshot));
        return result;
    }
    
    // 读取 Data Manifest（Base + Delta）
    public List<ManifestFileMeta> readDataManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(read(snapshot.baseManifestList(), 
                           snapshot.baseManifestListSize()));
        result.addAll(readDeltaManifests(snapshot));
        return result;
    }
    
    // 读取 Delta Manifest
    public List<ManifestFileMeta> readDeltaManifests(Snapshot snapshot) {
        return read(snapshot.deltaManifestList(), 
                   snapshot.deltaManifestListSize());
    }
    
    // 读取 Changelog Manifest
    public List<ManifestFileMeta> readChangelogManifests(Snapshot snapshot) {
        return snapshot.changelogManifestList() == null
                ? Collections.emptyList()
                : read(snapshot.changelogManifestList(), 
                      snapshot.changelogManifestListSize());
    }
    
    // 写入 ManifestList（原子操作）
    public Pair<String, Long> write(List<ManifestFileMeta> metas) {
        return super.writeWithoutRolling(metas.iterator());
    }
}
```

### 3.2 ManifestFileMeta 结构

`ManifestFileMeta` 是 `ManifestFile` 的元信息。

```java
public class ManifestFileMeta {
    // 文件名（相对路径）
    private final String fileName;
    
    // 文件大小
    private final long fileSize;
    
    // 新增文件数
    private final long numAddedFiles;
    
    // 删除文件数
    private final long numDeletedFiles;
    
    // 分区统计信息（用于过滤）
    private final SimpleStats partitionStats;
    
    // Schema ID
    private final long schemaId;
    
    // Bucket 范围（用于过滤）
    @Nullable private final Integer minBucket;
    @Nullable private final Integer maxBucket;
    
    // Level 范围（用于过滤）
    @Nullable private final Integer minLevel;
    @Nullable private final Integer maxLevel;
}
```

#### 统计信息的作用

这些统计信息用于**在读取前过滤不需要的 ManifestFile**：

1. **Bucket 过滤**：如果查询指定了 bucket，可以跳过不相关的 manifest
2. **Level 过滤**：某些查询只关心特定 level 的文件
3. **分区过滤**：根据分区谓词过滤 manifest

### 3.3 ManifestFile 实现

`ManifestFile` 存储多个 `ManifestEntry`。

#### 核心实现

```java
public class ManifestFile extends ObjectsFile<ManifestEntry> {
    private final SchemaManager schemaManager;
    private final RowType partitionType;
    private final FormatWriterFactory writerFactory;
    private final long suggestedFileSize;  // 建议文件大小（用于 rolling）
    
    // 写入多个 Entry（可能生成多个 ManifestFile）
    public List<ManifestFileMeta> write(List<ManifestEntry> entries) {
        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = 
            createRollingWriter();
        try {
            writer.write(entries);
            writer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return writer.result();
    }
    
    // ManifestEntry 写入器
    public class ManifestEntryWriter 
            extends SingleFileWriter<ManifestEntry, ManifestFileMeta> {
        
        private final SimpleStatsCollector partitionStatsCollector;
        private long numAddedFiles = 0;
        private long numDeletedFiles = 0;
        private long schemaId = Long.MIN_VALUE;
        private int minBucket = Integer.MAX_VALUE;
        private int maxBucket = Integer.MIN_VALUE;
        private int minLevel = Integer.MAX_VALUE;
        private int maxLevel = Integer.MIN_VALUE;
        
        @Override
        public void write(ManifestEntry entry) throws IOException {
            super.write(entry);
            
            // 统计信息
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
            }
            
            schemaId = Math.max(schemaId, entry.file().schemaId());
            minBucket = Math.min(minBucket, entry.bucket());
            maxBucket = Math.max(maxBucket, entry.bucket());
            minLevel = Math.min(minLevel, entry.level());
            maxLevel = Math.max(maxLevel, entry.level());
            
            // 收集分区统计信息
            partitionStatsCollector.collect(entry.partition());
        }
        
        @Override
        public ManifestFileMeta result() throws IOException {
            return new ManifestFileMeta(
                path.getName(),
                outputBytes,
                numAddedFiles,
                numDeletedFiles,
                partitionStatsSerializer.toBinaryAllMode(
                    partitionStatsCollector.extract()),
                numAddedFiles + numDeletedFiles > 0 
                    ? schemaId 
                    : schemaManager.latest().get().id(),
                minBucket,
                maxBucket,
                minLevel,
                maxLevel
            );
        }
    }
}
```

### 3.4 ManifestEntry 详解

`ManifestEntry` 表示单个数据文件的变更（ADD 或 DELETE）。

#### 数据结构

```java
public interface ManifestEntry extends FileEntry {
    // Schema 定义
    RowType SCHEMA = new RowType(
        false,
        Arrays.asList(
            new DataField(0, "_KIND", new TinyIntType(false)),       // ADD/DELETE
            new DataField(1, "_PARTITION", newBytesType(false)),     // 分区
            new DataField(2, "_BUCKET", new IntType(false)),         // Bucket
            new DataField(3, "_TOTAL_BUCKETS", new IntType(false)),  // 总 Bucket 数
            new DataField(4, "_FILE", DataFileMeta.SCHEMA)           // 文件元数据
        )
    );
    
    // 核心方法
    FileKind kind();           // ADD 或 DELETE
    BinaryRow partition();     // 分区信息
    int bucket();              // Bucket 编号
    int totalBuckets();        // 总 Bucket 数
    DataFileMeta file();       // 数据文件元数据
    Identifier identifier();   // 文件唯一标识
}
```

#### 实现类：PojoManifestEntry

```java
public class PojoManifestEntry implements ManifestEntry {
    private final FileKind kind;
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;
    
    @Override
    public Identifier identifier() {
        return new Identifier(
            partition,
            bucket,
            file.level(),
            file.fileName(),
            file.extraFiles(),
            file.embeddedIndex(),
            externalPath()
        );
    }
}
```

#### Entry 合并逻辑

`ManifestEntry` 的合并是 Paimon 的核心逻辑之一：

```java
public static void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
    for (T entry : entries) {
        Identifier identifier = entry.identifier();
        switch (entry.kind()) {
            case ADD:
                // ADD：直接添加
                Preconditions.checkState(
                    !map.containsKey(identifier),
                    "Trying to add file %s which is already added.",
                    identifier
                );
                map.put(identifier, entry);
                break;
                
            case DELETE:
                // DELETE：如果之前有 ADD，直接删除；否则保留 DELETE
                if (map.containsKey(identifier)) {
                    map.remove(identifier);
                } else {
                    map.put(identifier, entry);
                }
                break;
        }
    }
}
```

**合并原理**：
1. 如果同一个文件先 ADD 后 DELETE，最终结果为空（两者抵消）
2. 如果只有 DELETE，说明文件在之前的 Manifest 中，需要保留 DELETE 标记
3. 如果只有 ADD，说明是新增文件

### 3.5 DataFileMeta 结构

`DataFileMeta` 是实际数据文件的元信息。

```java
public interface DataFileMeta {
    // Schema 定义（字段众多，这里列出核心字段）
    RowType SCHEMA = new RowType(
        false,
        Arrays.asList(
            new DataField(0, "_FILE_NAME", newStringType(false)),
            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
            new DataField(2, "_ROW_COUNT", new BigIntType(false)),
            new DataField(3, "_MIN_KEY", newBytesType(false)),
            new DataField(4, "_MAX_KEY", newBytesType(false)),
            new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA),
            new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA),
            new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)),
            new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)),
            new DataField(9, "_SCHEMA_ID", new BigIntType(false)),
            new DataField(10, "_LEVEL", new IntType(false)),
            new DataField(11, "_EXTRA_FILES", 
                         new ArrayType(false, newStringType(false))),
            new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()),
            new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)),
            new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)),
            new DataField(15, "_FILE_SOURCE", new TinyIntType(true)),
            // ... 更多字段
        )
    );
    
    // 核心字段
    String fileName();
    long fileSize();
    long rowCount();
    BinaryRow minKey();
    BinaryRow maxKey();
    SimpleStats keyStats();
    SimpleStats valueStats();
    long minSequenceNumber();
    long maxSequenceNumber();
    long schemaId();
    int level();
}
```

---

## 4. 写入和提交流程

### 4.1 完整提交流程图

```
┌──────────────┐
│  Writer      │
│  写入数据     │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│ 生成 DataFile    │
│ (ORC/Parquet等)  │
└──────┬───────────┘
       │
       ▼
┌────────────────────────┐
│ 创建 ManifestEntry     │
│ (包含 DataFileMeta)    │
└──────┬─────────────────┘
       │
       ▼
┌────────────────────────┐
│ 收集所有 Entry         │
│ 准备提交               │
└──────┬─────────────────┘
       │
       ▼
┌────────────────────────┐
│ FileStoreCommitImpl    │
│ .commit()              │
└──────┬─────────────────┘
       │
       ├──► 1. 读取最新 Snapshot
       │
       ├──► 2. 冲突检测
       │
       ├──► 3. 合并 Base + Delta
       │
       ├──► 4. 写入新的 ManifestFile
       │
       ├──► 5. 写入新的 ManifestList
       │
       ├──► 6. 创建新的 Snapshot
       │
       └──► 7. 原子提交 Snapshot
```

### 4.2 FileStoreCommitImpl 核心代码

```java
public class FileStoreCommitImpl implements FileStoreCommit {
    
    @Override
    public int commit(ManifestCommittable committable, boolean checkAppendFiles) {
        // 1. 收集所有变更
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        
        collectChanges(
            committable.fileCommittables(),
            appendTableFiles,
            appendChangelog,
            compactTableFiles,
            compactChangelog,
            appendHashIndexFiles,
            compactDvIndexFiles
        );
        
        // 2. 获取最新 Snapshot
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        
        // 3. 生成新的 Snapshot ID
        long newSnapshotId = latestSnapshot == null ? 1 : latestSnapshot.id() + 1;
        
        // 4. 合并所有变更
        List<ManifestEntry> deltaFiles = new ArrayList<>();
        deltaFiles.addAll(appendTableFiles);
        deltaFiles.addAll(compactTableFiles);
        
        // 5. 读取 Base Manifests
        List<SimpleFileEntry> baseEntries = new ArrayList<>();
        if (latestSnapshot != null) {
            List<ManifestFileMeta> baseManifests = 
                manifestList.readDataManifests(latestSnapshot);
            // 合并 base entries
            mergeEntries(manifestFile, baseManifests, baseEntries);
        }
        
        // 6. 决定是否需要重新生成 Base
        Pair<String, Long> baseManifestList;
        if (shouldGenerateNewBase(baseEntries, deltaFiles)) {
            // 合并 Base + Delta 生成新的 Base
            List<ManifestEntry> allEntries = new ArrayList<>(baseEntries);
            mergeEntries(deltaFiles, allEntries);
            baseManifestList = manifestList.write(manifestFile.write(allEntries));
        } else {
            // 复用上一个 Base
            baseManifestList = Pair.of(
                latestSnapshot.baseManifestList(),
                latestSnapshot.baseManifestListSize()
            );
        }
        
        // 7. 写入 Delta Manifest
        Pair<String, Long> deltaManifestList = 
            manifestList.write(manifestFile.write(deltaFiles));
        
        // 8. 写入 Changelog Manifest（如果有）
        Pair<String, Long> changelogManifestList = null;
        if (!changelogFiles.isEmpty()) {
            changelogManifestList = 
                manifestList.write(manifestFile.write(changelogFiles));
        }
        
        // 9. 创建新 Snapshot
        Snapshot newSnapshot = new Snapshot(
            newSnapshotId,
            latestSchemaId,
            baseManifestList.getLeft(),
            baseManifestList.getRight(),
            deltaManifestList.getKey(),
            deltaManifestList.getRight(),
            changelogManifestList == null ? null : changelogManifestList.getKey(),
            changelogManifestList == null ? null : changelogManifestList.getRight(),
            indexManifest,
            commitUser,
            identifier,
            commitKind,
            System.currentTimeMillis(),
            logOffsets,
            totalRecordCount,
            deltaRecordCount,
            recordCount(changelogFiles),
            currentWatermark,
            statsFileName,
            properties.isEmpty() ? null : properties,
            nextRowIdStart
        );
        
        // 10. 原子提交 Snapshot
        boolean committed = snapshotCommit.commit(
            newSnapshot, 
            branch, 
            partitionStatistics
        );
        
        if (committed) {
            // 11. 提交成功，更新 hint 文件
            snapshotManager.commitLatestHint(newSnapshotId);
        }
        
        return committed ? 1 : 0;
    }
}
```

### 4.3 Snapshot 原子提交机制

Paimon 使用 **文件重命名** 实现原子提交：

```java
public class RenamingSnapshotCommit implements SnapshotCommit {
    
    @Override
    public boolean commit(Snapshot snapshot, String branch, 
                         List<PartitionStatistics> statistics) throws Exception {
        Path newSnapshotPath = snapshotManager.snapshotPath(snapshot.id());
        
        Callable<Boolean> callable = () -> {
            // 原子写入
            boolean committed = fileIO.tryToWriteAtomic(
                newSnapshotPath, 
                snapshot.toJson()
            );
            
            if (committed) {
                // 更新 latest hint
                snapshotManager.commitLatestHint(snapshot.id());
            }
            return committed;
        };
        
        // 使用锁保护（对象存储需要）
        return lock.runWithLock(() ->
            !fileIO.exists(newSnapshotPath) && callable.call()
        );
    }
}
```

**原子性保证**：
1. **HDFS/本地文件系统**：`rename` 操作是原子的
2. **对象存储（S3/OSS）**：需要外部锁（通过 DynamoDB/ZooKeeper 等）

### 4.4 Base Manifest 的生成策略

```java
private boolean shouldGenerateNewBase(
        List<SimpleFileEntry> baseEntries, 
        List<ManifestEntry> deltaFiles) {
    
    // 策略 1: Delta 文件数超过阈值
    if (deltaFiles.size() > maxDeltaFiles) {
        return true;
    }
    
    // 策略 2: Delta 占 Base 的比例超过阈值
    double deltaRatio = (double) deltaFiles.size() / baseEntries.size();
    if (deltaRatio > maxDeltaRatio) {
        return true;
    }
    
    // 策略 3: Manifest 文件数超过阈值
    int totalManifests = baseManifests.size() + deltaManifests.size();
    if (totalManifests > maxManifests) {
        return true;
    }
    
    return false;
}
```

---

## 5. 读取流程分析

### 5.1 完整读取流程图

```
┌──────────────┐
│  Reader      │
│  发起读取请求 │
└──────┬───────┘
       │
       ▼
┌────────────────────┐
│ 1. 选择 Snapshot   │
│ (latest/指定ID/时间戳)│
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 2. 读取 Snapshot   │
│    文件 (JSON)     │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 3. 确定扫描模式    │
│ (ALL/DELTA/CHANGELOG)│
└──────┬─────────────┘
       │
       ├──► ALL: Base + Delta
       ├──► DELTA: 仅 Delta
       └──► CHANGELOG: Changelog
       │
       ▼
┌────────────────────┐
│ 4. 读取 ManifestList│
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 5. 过滤 ManifestFileMeta│
│ (根据分区/bucket/level)│
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 6. 读取 ManifestFile│
│    (并行读取)       │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 7. 合并 Entry      │
│ (处理 ADD/DELETE)  │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 8. 过滤 DataFile   │
│ (根据谓词)          │
└──────┬─────────────┘
       │
       ▼
┌────────────────────┐
│ 9. 读取 DataFile   │
│    返回数据         │
└────────────────────┘
```

### 5.2 ManifestsReader 实现

```java
public class ManifestsReader {
    
    public Result read(@Nullable Snapshot specifiedSnapshot, ScanMode scanMode) {
        // 1. 获取 Snapshot
        Snapshot snapshot = specifiedSnapshot == null 
            ? snapshotManager.latestSnapshot() 
            : specifiedSnapshot;
            
        if (snapshot == null) {
            return emptyResult();
        }
        
        // 2. 根据 ScanMode 读取 Manifest
        List<ManifestFileMeta> manifests = readManifests(snapshot, scanMode);
        
        // 3. 过滤 ManifestFileMeta
        List<ManifestFileMeta> filtered = manifests.stream()
            .filter(this::filterManifestFileMeta)
            .collect(Collectors.toList());
            
        return new Result(snapshot, manifests, filtered);
    }
    
    private List<ManifestFileMeta> readManifests(
            Snapshot snapshot, ScanMode scanMode) {
        ManifestList manifestList = manifestListFactory.create();
        
        switch (scanMode) {
            case ALL:
                // 读取 Base + Delta
                return manifestList.readDataManifests(snapshot);
                
            case DELTA:
                // 只读取 Delta
                return manifestList.readDeltaManifests(snapshot);
                
            case CHANGELOG:
                // 读取 Changelog
                return manifestList.readChangelogManifests(snapshot);
                
            default:
                throw new UnsupportedOperationException(
                    "Unknown scan kind " + scanMode.name()
                );
        }
    }
    
    // 过滤 ManifestFileMeta
    private boolean filterManifestFileMeta(ManifestFileMeta manifest) {
        // 1. Bucket 过滤
        if (specifiedBucket != null) {
            Integer minBucket = manifest.minBucket();
            Integer maxBucket = manifest.maxBucket();
            if (minBucket != null && maxBucket != null) {
                if (specifiedBucket < minBucket || 
                    specifiedBucket > maxBucket) {
                    return false;
                }
            }
        }
        
        // 2. Level 过滤
        if (specifiedLevel != null) {
            Integer minLevel = manifest.minLevel();
            Integer maxLevel = manifest.maxLevel();
            if (minLevel != null && maxLevel != null) {
                if (specifiedLevel < minLevel || 
                    specifiedLevel > maxLevel) {
                    return false;
                }
            }
        }
        
        // 3. 分区过滤
        if (partitionFilter != null) {
            SimpleStats stats = manifest.partitionStats();
            return partitionFilter.test(
                manifest.numAddedFiles() + manifest.numDeletedFiles(),
                stats.minValues(),
                stats.maxValues(),
                stats.nullCounts()
            );
        }
        
        return true;
    }
}
```

### 5.3 并行读取 Manifest

Paimon 支持并行读取多个 ManifestFile 以加速扫描：

```java
public static Iterable<ManifestEntry> readManifestEntries(
        ManifestFile manifestFile,
        List<ManifestFileMeta> manifestFiles,
        @Nullable Integer manifestReadParallelism) {
    
    return sequentialBatchedExecute(
        file -> manifestFile.read(file.fileName(), file.fileSize()),
        manifestFiles,
        manifestReadParallelism
    );
}
```

### 5.4 Entry 合并示例

假设有以下 Entry 序列：

```
Base Manifests:
  - ADD file1 (partition=2023-01-01, bucket=0)
  - ADD file2 (partition=2023-01-01, bucket=1)
  
Delta Manifests:
  - DELETE file1
  - ADD file3 (partition=2023-01-02, bucket=0)
```

合并后的结果：

```
Final Entries:
  - ADD file2 (partition=2023-01-01, bucket=1)
  - ADD file3 (partition=2023-01-02, bucket=0)
```

**合并逻辑**：
- `file1` 的 ADD 和 DELETE 抵消，最终被移除
- `file2` 和 `file3` 保留

---

## 6. 文件组织结构

### 6.1 完整目录结构

```
{table_path}/
├── snapshot/
│   ├── snapshot-1              # Snapshot 文件（JSON）
│   ├── snapshot-2
│   ├── snapshot-3
│   ├── EARLIEST                # Earliest hint 文件
│   └── LATEST                  # Latest hint 文件
│
├── manifest/
│   ├── manifest-list-{uuid}    # ManifestList 文件
│   ├── manifest-{uuid}         # ManifestFile 文件
│   └── index-manifest-{uuid}   # Index Manifest 文件
│
├── schema/
│   ├── schema-0                # Schema 文件（JSON）
│   └── schema-1
│
├── bucket-0/
│   ├── data-{uuid}.parquet     # 数据文件
│   ├── data-{uuid}.parquet
│   └── ...
│
├── bucket-1/
│   └── ...
│
└── branch-{name}/              # 分支目录（可选）
    ├── snapshot/
    ├── manifest/
    └── ...
```

### 6.2 文件命名规则

| 文件类型 | 命名规则 | 示例 |
|---------|---------|------|
| Snapshot | `snapshot-{id}` | `snapshot-5` |
| ManifestList | `manifest-list-{uuid}` | `manifest-list-a1b2c3d4-e5f6...` |
| ManifestFile | `manifest-{uuid}` | `manifest-b2c3d4e5-f6a7...` |
| DataFile | `data-{uuid}.{ext}` | `data-c3d4e5f6.parquet` |
| Schema | `schema-{id}` | `schema-0` |

### 6.3 文件大小控制

#### ManifestFile 大小控制

```java
public class ManifestFile extends ObjectsFile<ManifestEntry> {
    private final long suggestedFileSize;  // 默认 8MB
    
    public RollingFileWriter<ManifestEntry, ManifestFileMeta> createRollingWriter() {
        return new RollingFileWriter<>(
            () -> new ManifestEntryWriter(...),
            suggestedFileSize  // 超过此大小会 rolling 生成新文件
        );
    }
}
```

#### 为什么需要 Rolling？

1. **避免单个文件过大**：加速读取
2. **并行化**：多个小文件可以并行处理
3. **部分读取**：只读取需要的文件

---

## 7. 关键设计要点

### 7.1 为什么需要 Base + Delta 设计？

#### 问题：如果只有 Delta

```
Snapshot 1: Delta1 (file1, file2)
Snapshot 2: Delta2 (file3, -file1)  # -表示删除
Snapshot 3: Delta3 (file4)
...
Snapshot 100: Delta100 (file150)
```

**读取 Snapshot 100 需要**：
- 读取 Delta1 ~ Delta100（100 个 ManifestList）
- 合并所有 Entry（性能很差）

#### 解决：Base + Delta

```
Snapshot 1: Base1 (file1, file2)
Snapshot 2: Base1 + Delta2 (file3, -file1)
Snapshot 3: Base1 + Delta3 (file4)
...
Snapshot 10: Base10 (file2, file3, file4, ..., file15)  # 定期重新生成 Base
Snapshot 11: Base10 + Delta11 (file16)
```

**读取 Snapshot 11 需要**：
- 读取 Base10 + Delta11（2 个 ManifestList）
- 合并少量 Entry（性能好）

### 7.2 为什么使用 ManifestList？

#### 问题：如果直接在 Snapshot 中列出所有 ManifestFile

```json
{
  "id": 100,
  "manifestFiles": [
    "manifest-1",
    "manifest-2",
    ...
    "manifest-1000"  // 可能有很多个
  ]
}
```

**问题**：
- Snapshot 文件会非常大
- 每次提交需要重写整个 Snapshot

#### 解决：使用 ManifestList

```json
{
  "id": 100,
  "baseManifestList": "manifest-list-abc",     // 指向一个文件
  "deltaManifestList": "manifest-list-def"     // 指向一个文件
}
```

**优点**：
- Snapshot 文件保持小巧
- ManifestList 可以复用
- 分层设计，清晰易维护

### 7.3 为什么需要统计信息（SimpleStats）？

#### 场景：分区过滤查询

```sql
SELECT * FROM t WHERE partition_date = '2023-01-01'
```

**没有统计信息**：
- 需要读取所有 ManifestFile
- 然后过滤 Entry

**有统计信息**：
- 在 ManifestFileMeta 中记录分区范围
- 直接跳过不相关的 ManifestFile
- 大大减少 I/O

### 7.4 并发控制：如何处理并发提交？

#### 场景

```
Writer A: 准备提交 Snapshot 6（基于 Snapshot 5）
Writer B: 准备提交 Snapshot 6（基于 Snapshot 5）
```

#### 解决：乐观并发控制 + 原子操作

```java
// FileStoreCommitImpl.commit()
while (attempts < maxRetries) {
    try {
        // 1. 读取最新 Snapshot
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        long newSnapshotId = latestSnapshot.id() + 1;
        
        // 2. 准备新 Snapshot
        Snapshot newSnapshot = buildSnapshot(newSnapshotId, ...);
        
        // 3. 原子提交（如果文件已存在则失败）
        boolean committed = snapshotCommit.commit(newSnapshot);
        
        if (committed) {
            return;  // 成功
        } else {
            // 失败，重试
            attempts++;
        }
    } catch (Exception e) {
        // 处理异常
    }
}
```

**关键点**：
1. **读取最新 Snapshot**：确保基于最新状态
2. **原子提交**：使用 `tryToWriteAtomic` 确保只有一个成功
3. **重试机制**：失败后重新读取最新状态并重试

### 7.5 Snapshot 过期和清理

#### 过期策略

```java
public class SnapshotExpiration {
    private final int numRetainedMin;        // 最少保留 Snapshot 数
    private final int numRetainedMax;        // 最多保留 Snapshot 数
    private final long snapshotTimeRetain;   // 时间保留（毫秒）
    
    public void expire() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        
        // 计算可以删除的 Snapshot
        List<Snapshot> expiredSnapshots = new ArrayList<>();
        for (long id = earliestSnapshotId; id < latestSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            
            // 1. 检查数量保留策略
            long remainingCount = latestSnapshotId - id;
            if (remainingCount <= numRetainedMin) {
                break;
            }
            
            // 2. 检查时间保留策略
            long age = System.currentTimeMillis() - snapshot.timeMillis();
            if (age < snapshotTimeRetain) {
                continue;
            }
            
            expiredSnapshots.add(snapshot);
        }
        
        // 删除过期的 Snapshot 和相关文件
        for (Snapshot snapshot : expiredSnapshots) {
            deleteSnapshot(snapshot);
        }
    }
    
    private void deleteSnapshot(Snapshot snapshot) {
        // 1. 标记需要删除的文件
        Set<String> filesToDelete = collectFilesToDelete(snapshot);
        
        // 2. 排除仍在使用的文件（被其他 Snapshot 引用）
        Set<String> filesInUse = collectFilesInUse();
        filesToDelete.removeAll(filesInUse);
        
        // 3. 删除文件
        for (String file : filesToDelete) {
            fileIO.deleteQuietly(new Path(file));
        }
        
        // 4. 删除 Snapshot 文件
        snapshotManager.deleteSnapshot(snapshot.id());
    }
}
```

---

## 8. Mini 版本实现建议

### 8.1 简化的架构

对于 Mini 版本，建议采用以下简化：

#### 核心类

```java
// 1. Snapshot（简化版）
class MiniSnapshot {
    long id;
    long schemaId;
    String manifestListFile;
    String commitUser;
    long commitTime;
    
    String toJson() { ... }
    static MiniSnapshot fromJson(String json) { ... }
}

// 2. ManifestFileMeta（简化版）
class MiniManifestFileMeta {
    String fileName;
    long fileSize;
    long numAddedFiles;
    long numDeletedFiles;
}

// 3. ManifestEntry（简化版）
class MiniManifestEntry {
    enum Kind { ADD, DELETE }
    
    Kind kind;
    String partition;
    int bucket;
    DataFileMeta file;
}

// 4. DataFileMeta（简化版）
class MiniDataFileMeta {
    String fileName;
    long fileSize;
    long rowCount;
    int level;
}
```

### 8.2 简化的目录结构

```
{table_path}/
├── snapshots/
│   ├── snapshot-1.json
│   ├── snapshot-2.json
│   └── latest                 # 记录最新 Snapshot ID
│
├── manifests/
│   ├── manifest-list-1.avro
│   └── manifest-1.avro
│
└── data/
    ├── partition=2023-01-01/
    │   ├── bucket-0/
    │   │   └── data-1.parquet
    │   └── bucket-1/
    │       └── data-2.parquet
    └── partition=2023-01-02/
        └── ...
```

### 8.3 简化的实现逻辑

#### 8.3.1 简化写入流程

```java
class MiniWriter {
    
    void commit(List<MiniManifestEntry> entries) {
        // 1. 读取最新 Snapshot
        MiniSnapshot latestSnapshot = readLatestSnapshot();
        long newSnapshotId = latestSnapshot == null ? 1 : latestSnapshot.id + 1;
        
        // 2. 读取旧的 Manifest（如果有）
        List<MiniManifestEntry> oldEntries = new ArrayList<>();
        if (latestSnapshot != null) {
            oldEntries = readManifestEntries(latestSnapshot.manifestListFile);
        }
        
        // 3. 合并 Entry
        Map<String, MiniManifestEntry> mergedEntries = new HashMap<>();
        for (MiniManifestEntry entry : oldEntries) {
            mergedEntries.put(entry.file.fileName, entry);
        }
        for (MiniManifestEntry entry : entries) {
            if (entry.kind == Kind.ADD) {
                mergedEntries.put(entry.file.fileName, entry);
            } else {
                mergedEntries.remove(entry.file.fileName);
            }
        }
        
        // 4. 写入 Manifest
        String manifestFile = writeManifest(new ArrayList<>(mergedEntries.values()));
        
        // 5. 写入 Snapshot
        MiniSnapshot newSnapshot = new MiniSnapshot(
            newSnapshotId,
            1,  // schemaId
            manifestFile,
            "user",
            System.currentTimeMillis()
        );
        
        // 6. 原子提交
        Path snapshotPath = new Path("snapshots/snapshot-" + newSnapshotId + ".json");
        fileIO.writeFile(snapshotPath, newSnapshot.toJson());
        
        // 7. 更新 latest
        fileIO.writeFile(new Path("snapshots/latest"), String.valueOf(newSnapshotId));
    }
}
```

#### 8.3.2 简化读取流程

```java
class MiniReader {
    
    List<DataFile> read(Long snapshotId, Predicate predicate) {
        // 1. 读取 Snapshot
        MiniSnapshot snapshot = snapshotId == null 
            ? readLatestSnapshot() 
            : readSnapshot(snapshotId);
            
        // 2. 读取 Manifest
        List<MiniManifestEntry> entries = 
            readManifestEntries(snapshot.manifestListFile);
        
        // 3. 过滤 Entry
        List<MiniManifestEntry> filtered = entries.stream()
            .filter(entry -> entry.kind == Kind.ADD)
            .filter(entry -> predicate.test(entry))
            .collect(Collectors.toList());
        
        // 4. 读取数据文件
        List<DataFile> results = new ArrayList<>();
        for (MiniManifestEntry entry : filtered) {
            DataFile file = readDataFile(entry.file.fileName);
            results.add(file);
        }
        
        return results;
    }
}
```

### 8.4 可选的进阶功能

#### 8.4.1 添加 Delta Manifest

```java
class MiniSnapshot {
    String baseManifestList;   // 所有文件
    String deltaManifestList;  // 增量文件
    
    // 每 N 次提交重新生成 Base
    static final int BASE_INTERVAL = 10;
}
```

#### 8.4.2 添加统计信息

```java
class MiniManifestFileMeta {
    String fileName;
    long fileSize;
    long numAddedFiles;
    long numDeletedFiles;
    
    // 统计信息（用于过滤）
    String minPartition;
    String maxPartition;
    int minBucket;
    int maxBucket;
}
```

#### 8.4.3 添加并发控制

```java
class MiniWriter {
    
    void commitWithRetry(List<MiniManifestEntry> entries) {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                if (tryCommit(entries)) {
                    return;  // 成功
                }
            } catch (FileAlreadyExistsException e) {
                // 冲突，重试
                Thread.sleep(100);
            }
        }
        throw new RuntimeException("Commit failed after retries");
    }
    
    boolean tryCommit(List<MiniManifestEntry> entries) {
        // ... 准备 Snapshot
        
        // 原子写入（如果文件存在则失败）
        return fileIO.createNewFile(snapshotPath, snapshot.toJson());
    }
}
```

### 8.5 测试用例建议

```java
class MiniPaimonTest {
    
    @Test
    void testBasicWriteAndRead() {
        // 1. 写入数据
        MiniWriter writer = new MiniWriter();
        writer.write("partition1", 0, List.of(row1, row2));
        writer.commit();
        
        // 2. 读取数据
        MiniReader reader = new MiniReader();
        List<Row> rows = reader.read(null, Predicate.alwaysTrue());
        
        assertEquals(2, rows.size());
    }
    
    @Test
    void testSnapshotIsolation() {
        MiniWriter writer = new MiniWriter();
        
        // 提交 Snapshot 1
        writer.write("partition1", 0, List.of(row1));
        writer.commit();
        
        // 提交 Snapshot 2
        writer.write("partition1", 0, List.of(row2));
        writer.commit();
        
        // 读取 Snapshot 1（应该只有 row1）
        MiniReader reader1 = new MiniReader();
        List<Row> rows1 = reader1.read(1L, Predicate.alwaysTrue());
        assertEquals(1, rows1.size());
        
        // 读取 Snapshot 2（应该有 row1 和 row2）
        MiniReader reader2 = new MiniReader();
        List<Row> rows2 = reader2.read(2L, Predicate.alwaysTrue());
        assertEquals(2, rows2.size());
    }
    
    @Test
    void testManifestMerge() {
        MiniWriter writer = new MiniWriter();
        
        // 添加 file1
        ManifestEntry add1 = ManifestEntry.add(file1);
        writer.commit(List.of(add1));
        
        // 删除 file1
        ManifestEntry delete1 = ManifestEntry.delete(file1);
        writer.commit(List.of(delete1));
        
        // 读取最新 Snapshot（应该为空）
        MiniReader reader = new MiniReader();
        List<DataFile> files = reader.listFiles(null);
        assertEquals(0, files.size());
    }
    
    @Test
    void testConcurrentCommit() throws Exception {
        MiniWriter writer1 = new MiniWriter();
        MiniWriter writer2 = new MiniWriter();
        
        // 并发提交
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<Boolean> future1 = executor.submit(() -> {
            writer1.commit(List.of(entry1));
            return true;
        });
        Future<Boolean> future2 = executor.submit(() -> {
            writer2.commit(List.of(entry2));
            return true;
        });
        
        // 至少有一个成功
        boolean result1 = future1.get();
        boolean result2 = future2.get();
        assertTrue(result1 || result2);
        
        // 检查数据一致性
        MiniReader reader = new MiniReader();
        List<DataFile> files = reader.listFiles(null);
        assertTrue(files.size() >= 1);
    }
}
```

---

## 9. 总结

### 9.1 核心设计要点回顾

1. **四层元数据架构**
   - Snapshot → ManifestList → ManifestFile → ManifestEntry → DataFile
   - 每一层都有明确的职责和优化目标

2. **Base + Delta 设计**
   - 避免每次都扫描所有 Manifest
   - 定期合并减少元数据膨胀

3. **原子提交机制**
   - 使用文件重命名保证原子性
   - 乐观并发控制 + 重试机制

4. **增量读取优化**
   - Delta Manifest 支持流式读取
   - 统计信息支持元数据过滤

5. **MVCC 多版本并发**
   - 每次提交生成新 Snapshot
   - 支持时间旅行和回滚

### 9.2 与其他系统对比

| 特性 | Paimon | Iceberg | Hudi |
|-----|--------|---------|------|
| Snapshot 格式 | JSON | Avro | JSON |
| 元数据层次 | 4 层 | 4 层 | 3 层 |
| Base + Delta | ✅ | ✅ | ❌ |
| 原子提交 | Rename | Rename | Rename |
| 流式读取 | ✅ (Delta) | ✅ (Snapshot) | ✅ (增量查询) |

### 9.3 实现建议总结

**对于 Mini 版本**：
1. 先实现核心的 Snapshot + Manifest 机制
2. 使用简单的 JSON 序列化（避免复杂的 Avro/Parquet）
3. 只保留 base manifest，暂不实现 delta
4. 使用简单的文件锁实现并发控制
5. 重点关注正确性，性能优化可以后续迭代

**关键代码量估算**：
- `MiniSnapshot`: ~100 行
- `MiniManifest`: ~200 行
- `MiniWriter`: ~300 行
- `MiniReader`: ~200 行
- 总计：~800 行核心代码

通过循序渐进的实现，你可以深入理解 Paimon 的设计思想，为后续学习和优化打下坚实基础。

---

## 附录：关键源码位置

| 组件 | 源码路径 |
|-----|---------|
| Snapshot | `paimon-api/src/main/java/org/apache/paimon/Snapshot.java` |
| SnapshotManager | `paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java` |
| ManifestList | `paimon-core/src/main/java/org/apache/paimon/manifest/ManifestList.java` |
| ManifestFile | `paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFile.java` |
| ManifestEntry | `paimon-core/src/main/java/org/apache/paimon/manifest/ManifestEntry.java` |
| DataFileMeta | `paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java` |
| FileStoreCommit | `paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java` |
| ManifestsReader | `paimon-core/src/main/java/org/apache/paimon/operation/ManifestsReader.java` |

**文档版本**: 基于 Apache Paimon 主分支（2024年11月）
**文档作者**: 根据源码分析整理
**建议阅读时间**: 60-90 分钟

