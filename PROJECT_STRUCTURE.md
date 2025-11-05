# Mini Paimon - 项目结构说明

## 项目概述
Mini Paimon 是一个简化版的 Apache Paimon 实现，用于学习 LSM Tree 存储结构、索引管理、表元数据管理等核心概念。

## 当前阶段：阶段一 - 基础设施搭建 ✅

### 已完成的任务

#### 1. 项目结构初始化 ✅
- ✅ 创建 Maven 项目
- ✅ 定义包结构
- ✅ 添加依赖（Guava、Jackson、JUnit）

#### 2. 基础数据结构定义 ✅
- ✅ `DataType` 枚举（INT, LONG, STRING, BOOLEAN）
- ✅ `Field` 类（字段名、类型、是否可空）
- ✅ `Schema` 类（字段列表、主键定义）
- ✅ `Row` 类（数据行的表示）
- ✅ `RowKey` 类（主键的序列化表示）

#### 3. 工具类实现 ✅
- ✅ `PathFactory` - 文件路径管理
- ✅ `SerializationUtils` - JSON 序列化工具
- ✅ `IdGenerator` - ID 生成器

#### 4. 异常体系 ✅
- ✅ `MiniPaimonException` - 基础异常类
- ✅ `SchemaException` - Schema 相关异常
- ✅ `StorageException` - 存储相关异常

#### 5. 单元测试 ✅
- ✅ `SchemaTest` - Schema 测试
- ✅ `RowTest` - Row 测试
- ✅ `RowKeyTest` - RowKey 测试
- ✅ 所有测试通过（12/12）

## 项目目录结构

```
mini-paimon/
├── pom.xml                          # Maven 配置文件
├── .gitignore                       # Git 忽略配置
├── README.md                        # 项目说明（原始需求文档）
├── PROJECT_STRUCTURE.md             # 本文件
└── src/
    ├── main/
    │   ├── java/com/minipaimon/
    │   │   ├── metadata/            # 元数据管理模块 ✅
    │   │   │   ├── DataType.java    # 数据类型枚举
    │   │   │   ├── Field.java       # 字段定义
    │   │   │   ├── Schema.java      # 表结构
    │   │   │   ├── Row.java         # 数据行
    │   │   │   └── RowKey.java      # 主键序列化
    │   │   ├── storage/             # 存储引擎模块（待实现）
    │   │   ├── manifest/            # Manifest 管理模块（待实现）
    │   │   ├── snapshot/            # Snapshot 管理模块（待实现）
    │   │   ├── index/               # 索引模块（待实现）
    │   │   ├── sql/                 # SQL 解析模块（待实现）
    │   │   ├── exception/           # 异常类 ✅
    │   │   │   ├── MiniPaimonException.java
    │   │   │   ├── SchemaException.java
    │   │   │   └── StorageException.java
    │   │   └── utils/               # 工具类 ✅
    │   │       ├── PathFactory.java
    │   │       ├── SerializationUtils.java
    │   │       └── IdGenerator.java
    │   └── resources/
    │       └── logback.xml          # 日志配置
    └── test/
        └── java/com/minipaimon/
            └── metadata/            # 元数据模块测试 ✅
                ├── SchemaTest.java
                ├── RowTest.java
                └── RowKeyTest.java
```

## 核心组件说明

### 元数据模块 (metadata)

1. **DataType** - 数据类型枚举
   - INT: 32位整数
   - LONG: 64位长整数
   - STRING: 字符串（变长）
   - BOOLEAN: 布尔值

2. **Field** - 字段定义
   - name: 字段名
   - type: 数据类型
   - nullable: 是否可为空

3. **Schema** - 表结构
   - schemaId: Schema 版本ID
   - fields: 字段列表
   - primaryKeys: 主键字段列表
   - partitionKeys: 分区键字段列表

4. **Row** - 数据行
   - values: 字段值数组
   - 提供类型验证和非空约束检查

5. **RowKey** - 主键序列化
   - 将主键值序列化为字节数组
   - 支持字节序比较
   - 用于排序和索引

### 工具类模块 (utils)

1. **PathFactory** - 路径管理
   - 统一管理 warehouse 目录结构
   - 生成标准化的文件路径
   - 支持创建表目录结构

2. **SerializationUtils** - 序列化工具
   - JSON 序列化/反序列化
   - 文件读写支持
   - 使用 Jackson 库

3. **IdGenerator** - ID 生成器
   - 生成递增 ID
   - 生成 UUID
   - 生成 Manifest 文件 ID

## 技术栈

- **语言**: Java 11
- **构建工具**: Maven 3.x
- **依赖库**:
  - Guava 31.1-jre - 集合和工具类
  - Jackson 2.13.5 - JSON 序列化
  - SLF4J + Logback - 日志框架
  - JUnit 5.9.3 - 单元测试

## 编译和测试

```bash
# 清理并编译
mvn clean compile

# 运行测试
mvn test

# 打包
mvn package
```

## 下一步计划

### 阶段二：LSM Tree 存储引擎
- [ ] 任务 2.1：实现 MemTable（内存表）
- [ ] 任务 2.2：设计 SSTable 文件格式
- [ ] 任务 2.3：实现 SSTable Writer
- [ ] 任务 2.4：实现 SSTable Reader
- [ ] 任务 2.5：实现 Compaction 机制

### 阶段三：索引系统
- [ ] 任务 3.1：实现 Bloom Filter 索引
- [ ] 任务 3.2：实现稀疏索引

### 阶段四：元数据管理
- [ ] 任务 4.1：实现 Schema 管理器
- [ ] 任务 4.2：实现 Table 元数据管理

### 阶段五：Snapshot 机制
- [ ] 任务 5.1：实现 Snapshot 设计
- [ ] 任务 5.2：实现 Manifest 文件管理
- [ ] 任务 5.3：实现快照读取流程

### 阶段六：文件组织结构
- [ ] 任务 6.1：完善目录结构设计
- [ ] 任务 6.2：实现垃圾回收机制

## 设计理念

本项目遵循以下设计原则：

1. **简化但不简陋**：保留核心概念，简化实现细节
2. **模块化设计**：各模块职责清晰，低耦合
3. **可测试性**：每个模块都有对应的单元测试
4. **渐进式开发**：按阶段完成，每个阶段可独立验证
5. **代码质量**：遵循 Java 编码规范，注释完整

## 参考资料

- Apache Paimon 官方文档
- LSM Tree 论文和实现
- RocksDB 源码

---

**项目状态**: 🟢 阶段一完成，可以开始阶段二开发
**最后更新**: 2025-11-05
