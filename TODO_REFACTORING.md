# Mini Paimon 重构 TODO

## 已完成 ✓

### 核心提交系统
- [x] 创建 `operation` 包
- [x] 实现 `FileStoreCommitImpl`（两阶段提交）
- [x] 实现 `ManifestCommittable`（提交消息封装）
- [x] 实现 `SnapshotCommit` 接口
- [x] 实现 `AtomicRenameSnapshotCommit`（原子提交）
- [x] 实现 `CommitOptions`（配置管理）
- [x] 实现 `FileEntry`（文件变更合并和冲突检测）

### MVCC 并发控制
- [x] Snapshot ID 冲突检测
- [x] 文件删除冲突检测
- [x] LSM 键范围冲突检测（主键表）
- [x] 指数退避重试机制
- [x] 去重检查机制

### Metadata 管理
- [x] 简化 `Snapshot` 类（纯数据类）
- [x] 重构 `SnapshotManager`（只负责读写）
- [x] 简化 `ManifestList`
- [x] 简化 `ManifestFile`
- [x] 保持 `ManifestEntry` 和 `DataFileMeta`

### Base + Delta Manifest
- [x] Delta Manifest 写入
- [x] Base Manifest 合并逻辑
- [x] 自动触发合并（30 次 delta）

### 集成与适配
- [x] 重构 `TableCommit`（委托模式）
- [x] 更新 `FileSystemCatalog`
- [x] 修复 `FileStoreTable`
- [x] 修复 `DataTableScan`
- [x] 修复 `FileStoreTableScan`
- [x] 修复 `TransactionManager`
- [x] 修复 `BranchManager`

### 工具和依赖
- [x] 创建 `IdGenerator`
- [x] 创建 `ObjectsFile` 抽象类
- [x] 添加 Maven 依赖（Avro, Jackson, SLF4J）

### 文档和示例
- [x] 创建 `CommitExample`（基础提交示例）
- [x] 创建 `ConflictDetectionExample`（冲突检测示例）
- [x] 创建 `FullCommitFlowTest`（完整流程测试）
- [x] 创建 `REFACTORING_SUMMARY.md`（重构总结）

## 进行中 🔄

### 测试完善
- [ ] 运行所有示例代码，验证功能
- [ ] 修复潜在的编译错误
- [ ] 验证并发场景

## 待完成 📋

### 单元测试
- [ ] `FileStoreCommitImpl` 单元测试
  - [ ] 测试 snapshot ID 冲突
  - [ ] 测试文件删除冲突
  - [ ] 测试 LSM 键范围冲突
  - [ ] 测试重试机制
  - [ ] 测试去重机制
- [ ] `FileEntry` 单元测试
  - [ ] 测试 ADD/DELETE 合并
  - [ ] 测试冲突检测
- [ ] `AtomicRenameSnapshotCommit` 单元测试
  - [ ] 测试原子性
  - [ ] 测试并发冲突

### 集成测试
- [ ] 端到端提交流程测试
- [ ] 多线程并发写入测试
- [ ] Manifest 合并测试
- [ ] Snapshot 历史查询测试

### 性能优化
- [ ] Manifest 缓存优化
- [ ] 并行读取 Manifest
- [ ] 统计信息收集和使用
- [ ] 减少不必要的文件 I/O

### 监控和日志
- [ ] 添加详细的日志输出
- [ ] 添加性能指标收集
  - [ ] 提交延迟
  - [ ] 重试次数
  - [ ] 冲突率
  - [ ] Manifest 大小
- [ ] 添加错误统计

### 功能增强
- [ ] Changelog 支持
  - [ ] Changelog Manifest List
  - [ ] Changelog 写入
  - [ ] Changelog 读取
- [ ] 外部锁支持（S3/OSS）
  - [ ] 锁接口定义
  - [ ] DynamoDB 锁实现
  - [ ] ZooKeeper 锁实现
- [ ] Snapshot 过期和清理
  - [ ] 过期策略配置
  - [ ] 自动清理线程
  - [ ] 孤儿文件检测
- [ ] Schema 演进支持
  - [ ] Schema 版本管理
  - [ ] 字段添加/删除
  - [ ] 类型转换

### 代码质量
- [ ] 修复所有 linter warnings
- [ ] 添加 Javadoc 注释（核心类）
- [ ] 代码格式统一
- [ ] 异常处理规范化

### 文档完善
- [ ] API 文档
  - [ ] `FileStoreCommitImpl` API 文档
  - [ ] `SnapshotManager` API 文档
  - [ ] `ManifestCommittable` API 文档
- [ ] 架构设计文档
  - [ ] 详细的类图
  - [ ] 序列图
  - [ ] 状态机图
- [ ] 性能调优指南
- [ ] 故障排查指南

### Avro 迁移（可选）
- [ ] 设计 Avro Schema
  - [ ] ManifestEntry Schema
  - [ ] ManifestFileMeta Schema
  - [ ] DataFileMeta Schema
- [ ] 实现 Avro 序列化
- [ ] 实现 Avro 反序列化
- [ ] 向后兼容性测试
- [ ] 性能对比测试

## 已知问题 🐛

### 编译警告
- [ ] `MiniPaimonExample` 使用了废弃的 API
- [ ] `ManifestCacheManager` 有未使用的方法
- [ ] 一些类有未使用的字段

### 功能缺失
- [ ] `TableWrite` 还未完全集成新的提交流程
- [ ] 分支（Branch）功能还未完全测试
- [ ] 事务（Transaction）功能需要验证

## 优先级

### P0 - 关键（本周完成）
1. 运行并验证所有示例代码
2. 修复所有编译错误
3. 完成基础的单元测试

### P1 - 重要（本月完成）
1. 完成集成测试
2. 添加监控和日志
3. 完善文档

### P2 - 一般（可延后）
1. 性能优化
2. 功能增强（Changelog, 外部锁等）
3. Avro 迁移

### P3 - 可选（有时间再做）
1. 高级功能（Snapshot 过期、Schema 演进）
2. 性能调优指南
3. 完整的 Javadoc

## 时间估计

- P0 任务：2-3 天
- P1 任务：1-2 周
- P2 任务：2-4 周
- P3 任务：1-2 个月

## 责任人

- 核心提交系统：已完成
- 测试：待分配
- 文档：待分配
- 性能优化：待分配

## 里程碑

### Milestone 1: 核心功能完成 ✓
- 完成时间：当前
- 包含：两阶段提交、MVCC、冲突检测、Base+Delta

### Milestone 2: 测试和文档完善
- 目标：下周
- 包含：单元测试、集成测试、API 文档

### Milestone 3: 性能优化
- 目标：本月
- 包含：缓存优化、并行化、监控

### Milestone 4: 生产就绪
- 目标：下月
- 包含：所有功能完善、文档齐全、性能达标

## 参考资料

- Apache Paimon：https://github.com/apache/paimon
- 设计文档：
  - `Paimon-Snapshot-Manifest-深度分析.md`
  - `paimon-commit-mvcc-analysis.md`
  - `Paimon-分支表实现分析.md`
- 示例代码：`examples/` 目录

## 更新日志

- 2024-11-24: 完成核心提交系统重构
- 2024-11-24: 创建示例代码和文档

