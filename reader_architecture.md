# RecordReader 架构类图

## 核心类关系

```mermaid
graph TB
    SQLParser[SQLParser] --> DataTableRead
    DataTableRead --> RecordReaderFactory
    RecordReaderFactory --> SSTableRecordReader
    RecordReaderFactory --> MergeRecordReader
    
    SSTableRecordReader -.implements.-> FileRecordReader
    FileRecordReader -.extends.-> RecordReader
    MergeRecordReader -.implements.-> RecordReader
    
    DataTableRead --> DataTableScan
    DataTableScan --> ManifestEntry
    ManifestEntry --> DataFileMeta
    
    SSTableRecordReader --> Predicate
    SSTableRecordReader --> Projection
    SSTableRecordReader --> Schema
    
    DataTableRead --> Predicate
    DataTableRead --> Projection
```

## 读取流程

```mermaid
graph LR
    A[SQL查询] --> B[解析谓词和投影]
    B --> C[DataTableScan.plan]
    C --> D[获取文件列表]
    D --> E{文件级过滤}
    E -->|跳过| F[下一个文件]
    E -->|读取| G[创建RecordReader]
    G --> H{Block级过滤}
    H -->|跳过| I[下一个Block]
    H -->|读取| J[读取Block数据]
    J --> K[行级过滤]
    K --> L[投影]
    L --> M[返回结果]
    F --> E
    I --> H
```

## 接口设计

```mermaid
graph TB
    RecordReader[RecordReader Interface]
    FileRecordReader[FileRecordReader Interface]
    SSTableRecordReader[SSTableRecordReader]
    MergeRecordReader[MergeRecordReader]
    
    RecordReader --> |扩展| FileRecordReader
    FileRecordReader --> |实现| SSTableRecordReader
    RecordReader --> |实现| MergeRecordReader
    
    RecordReader --> |定义| readRecord
    RecordReader --> |定义| readBatch
    RecordReader --> |定义| close
    
    FileRecordReader --> |定义| seekToKey
    FileRecordReader --> |定义| hasNext
```

## 优化层次

```mermaid
graph TD
    A[查询请求] --> B[文件级过滤]
    B --> |minKey/maxKey| C{是否跳过文件?}
    C -->|是| D[跳过文件]
    C -->|否| E[Block级过滤]
    
    E --> |Block统计信息| F{是否跳过Block?}
    F -->|是| G[跳过Block]
    F -->|否| H[读取Block]
    
    H --> I[行级过滤]
    I --> |应用谓词| J[过滤行]
    J --> K[投影]
    K --> |选择字段| L[返回结果]
    
    D --> M[性能提升: 90%+]
    G --> N[性能提升: 50-70%]
    J --> O[性能提升: 20-40%]
```

## 数据流

```mermaid
graph LR
    A[SSTable文件] --> B[Block Iterator]
    B --> C{需要此Block?}
    C -->|否| D[跳过]
    C -->|是| E[读取Block]
    E --> F[反序列化]
    F --> G{过滤条件}
    G -->|不满足| H[丢弃行]
    G -->|满足| I[投影]
    I --> J[结果集]
    D --> B
    H --> F
```

## 类职责划分

### DataTableRead
- 文件级过滤（基于 DataFileMeta）
- 协调多个 FileRecordReader
- 支持批量和流式读取

### RecordReaderFactory
- 创建和配置 RecordReader
- 下推谓词和投影
- 封装 Reader 创建复杂性

### SSTableRecordReader
- Block 级懒加载
- Block 级过滤
- 行级过滤和投影
- 支持范围扫描

### MergeRecordReader
- 合并多个有序 Reader
- 保证结果有序性
- 用于多层 SSTable 合并读取
