# Mini-Paimon 分支表功能实现总结

## 已完成功能

### 一、核心层（paimon模块）✅

#### 1. BranchManager 接口与实现
- **BranchManager接口** (`com.mini.paimon.branch.BranchManager`)
  - 定义分支管理核心方法：createBranch、dropBranch、branches
  - 提供静态工具方法：normalizeBranch、isMainBranch、validateBranch、branchPath
  
- **FileSystemBranchManager实现** (`com.mini.paimon.branch.FileSystemBranchManager`)
  - 支持创建空分支
  - 支持从最新Snapshot创建分支（Tag支持待实现）
  - 支持删除分支
  - 支持列出所有分支

#### 2. Identifier类增强
- 支持解析 `table$branch_xxx` 格式的表名
- 支持解析 `table$branch_xxx$systemTable` 格式
- 提供getBranch()、getBranchNameOrDefault()、getSystemTableName()方法

#### 3. PathFactory分支支持
- 添加branch字段和构造函数
- 所有路径计算方法支持分支隔离
- 实现copyWithBranch()方法切换分支视图
- 分支路径规则：主分支使用表根目录，自定义分支使用`branch/branch-{name}`子目录

#### 4. SnapshotManager分支支持
- 添加branch字段
- 实现copyWithBranch()方法
- 路径计算自动使用分支路径

#### 5. SchemaManager分支支持
- 添加branch字段
- 实现copyWithBranch()方法
- 路径计算自动使用分支路径

#### 6. Catalog接口扩展
- 添加`createBranch(Identifier, String branch, String fromTag)`方法
- 添加`dropBranch(Identifier, String branch)`方法
- 添加`listBranches(Identifier)`方法
- FileSystemCatalog中完整实现上述方法

#### 7. Table接口和FileStoreTable增强
- 添加`branchManager()`方法返回BranchManager
- 添加`switchToBranch(String)`方法切换到指定分支
- switchToBranch实现支持Schema验证和PathFactory切换

#### 8. 测试覆盖
- 创建BranchManagerTest测试分支核心功能
- 测试用例包括：创建空分支、删除分支、列出分支、分支路径计算、分支验证、切换分支
- 所有126个核心测试通过 ✅

### 二、Flink模块（部分实现）✅

#### 已完成：
- **FlinkCatalog分支表访问支持**
  - 修改getTable()方法，自动解析table$branch_xxx格式
  - 当Identifier包含分支信息时，自动调用switchToBranch()

#### 待实现：
- CreateBranchProcedure - `CALL sys.create_branch('db.table', 'branch_name', 'tag_name')`
- DropBranchProcedure - `CALL sys.drop_branch('db.table', 'branch_name')`
- ListBranchesProcedure - `CALL sys.list_branches('db.table')`
- FlinkCatalog.getProcedure()方法注册

### 三、Spark模块（部分实现）✅

#### 已完成：
- **SparkCatalog分支表访问支持**
  - 修改loadTable()方法，自动解析table$branch_xxx格式
  - 当Identifier包含分支信息时，自动调用switchToBranch()

#### 待实现：
- CreateBranchProcedure - `CALL sys.create_branch('db.table', 'branch_name', 'tag_name')`
- DropBranchProcedure - `CALL sys.drop_branch('db.table', 'branch_name')`
- ListBranchesProcedure - `CALL sys.list_branches('db.table')`
- SparkCatalog.loadProcedure()方法注册

## 使用示例

### 1. 通过Java API使用

```java
// 获取Catalog
Catalog catalog = CatalogLoader.load("filesystem", context);

// 创建分支
Identifier table = new Identifier("test_db", "test_table");
catalog.createBranch(table, "dev", null);  // 创建空分支
catalog.createBranch(table, "prod", "tag1"); // 从tag创建分支（待tag支持）

// 列出分支
List<String> branches = catalog.listBranches(table);

// 删除分支
catalog.dropBranch(table, "dev");

// 访问分支表
Identifier branchTable = new Identifier("test_db", "test_table$branch_dev");
Table table = catalog.getTable(branchTable);

// 切换分支
Table mainTable = catalog.getTable(table);
Table devTable = mainTable.switchToBranch("dev");
```

### 2. 通过Flink SQL访问分支表

```sql
-- 查询分支表（使用table$branch_xxx语法）
SELECT * FROM `test_table$branch_dev`;

-- 向分支表写入数据
INSERT INTO `test_table$branch_dev` VALUES (1, 'data');
```

### 3. 通过Spark SQL访问分支表

```sql
-- 查询分支表
SELECT * FROM `test_table$branch_dev`;

-- 向分支表写入数据
INSERT INTO `test_table$branch_dev` VALUES (1, 'data');
```

## 设计要点

### 1. 路径隔离
- 主分支：`/warehouse/db/table/`
- 自定义分支：`/warehouse/db/table/branch/branch-{name}/`
- 每个分支维护独立的schema、snapshot、manifest目录

### 2. 零拷贝机制
- 创建分支时只复制元数据文件（Schema、Snapshot）
- 数据文件通过路径引用共享，避免冗余存储
- 新写入的数据存储在各自分支目录

### 3. copyWithBranch模式
- PathFactory、SnapshotManager、SchemaManager都实现了copyWithBranch()
- 通过创建新实例切换分支视图，保持不可变性
- 所有路径计算自动使用正确的分支路径

### 4. 分支命名规范
- 不能使用"main"（保留给主分支）
- 不能是纯数字（避免与snapshot ID混淆）
- 不能是空字符串
- 只能包含字母、数字、下划线和连字符

## 文件结构

```
/warehouse/db/table/
├── schema/          # 主分支schema
├── snapshot/        # 主分支snapshot
├── manifest/        # 主分支manifest
├── data/           # 主分支数据
└── branch/
    ├── branch-dev/
    │   ├── schema/
    │   ├── snapshot/
    │   ├── manifest/
    │   └── data/
    └── branch-test/
        ├── schema/
        ├── snapshot/
        ├── manifest/
        └── data/
```

## 测试结果

```
[INFO] Tests run: 126, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

所有核心模块测试通过，包括：
- 分支创建、删除、列举测试
- 分支路径计算测试
- 分支名称验证测试
- 分支切换测试
- 现有所有功能测试

## 后续工作

1. **Flink Procedure实现**
   - 创建ProcedureBase基类
   - 实现CreateBranchProcedure、DropBranchProcedure、ListBranchesProcedure
   - 在FlinkCatalog中注册procedures

2. **Spark Procedure实现**
   - 创建BaseProcedure基类
   - 实现CreateBranchProcedure、DropBranchProcedure、ListBranchesProcedure
   - 在SparkCatalog中注册procedures

3. **Tag支持**
   - 实现TagManager
   - 修改FileSystemBranchManager支持从Tag创建分支

4. **Fast-Forward合并**
   - 实现BranchManager.fastForward()方法
   - 支持将分支合并回主分支

5. **Fallback Branch机制**
   - 实现scan.fallback-branch配置支持
   - 实现FallbackReadFileStoreTable

6. **系统表支持**
   - 实现$branches系统表
   - 支持查询分支元数据

## 参考文档

- [Paimon-分支表实现分析.md](./Paimon-分支表实现分析.md) - 详细的设计和实现分析
- [architecture.md](./architecture.md) - 项目整体架构

