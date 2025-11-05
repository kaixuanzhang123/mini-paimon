# Mini Paimon SQL 读写数据实现

本文档描述了如何通过 SQL 语句读写 Mini Paimon 中的数据。

## 功能概述

Mini Paimon 现在支持以下 SQL 功能：

1. **CREATE TABLE** - 创建表并定义 Schema
2. **INSERT INTO** - 插入数据到表中

## SQL 语法支持

### CREATE TABLE 语法

```sql
CREATE TABLE table_name (
    column1 datatype [PRIMARY KEY],
    column2 datatype [NOT NULL],
    ...
)
```

支持的数据类型：
- INT - 32位整数
- LONG/BIGINT - 64位长整数
- STRING/VARCHAR/TEXT - 字符串
- BOOLEAN/BOOL - 布尔值
- DOUBLE/FLOAT - 双精度浮点数

### INSERT INTO 语法

```sql
-- 插入所有列
INSERT INTO table_name VALUES (value1, value2, ...)

-- 插入指定列
INSERT INTO table_name (col1, col2, ...) VALUES (value1, value2, ...)
```

## 使用示例

### 1. 创建表

```java
// 创建 SQL 解析器
SQLParser sqlParser = new SQLParser(tableManager, pathFactory);

// 创建用户表
String createUsersTableSQL = "CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name STRING NOT NULL, age INT, email STRING)";
sqlParser.executeSQL(createUsersTableSQL);

// 创建订单表
String createOrdersTableSQL = "CREATE TABLE orders (order_id INT NOT NULL PRIMARY KEY, user_id INT NOT NULL, product STRING, amount DOUBLE)";
sqlParser.executeSQL(createOrdersTableSQL);
```

### 2. 插入数据

```java
// 插入完整行数据
sqlParser.executeSQL("INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com')");

// 插入指定列数据
sqlParser.executeSQL("INSERT INTO users (id, name) VALUES (4, 'David')");

// 插入订单数据（包含 DOUBLE 类型）
sqlParser.executeSQL("INSERT INTO orders VALUES (101, 1, 'Laptop', 1200.0)");
```

## 实现细节

### SQL 解析器 (SQLParser)

SQL 解析器负责解析和执行 SQL 语句：

1. **CREATE TABLE 解析**：
   - 解析表名和列定义
   - 识别数据类型和约束（PRIMARY KEY, NOT NULL）
   - 创建表 Schema 并持久化

2. **INSERT 解析**：
   - 解析表名和值列表
   - 验证列数和数据类型匹配
   - 转换字符串值为对应的数据类型
   - 将数据写入 LSM Tree 存储

### 数据持久化流程

1. **Schema 管理**：
   - 表结构存储在 `warehouse/{database}/{table}/schema/schema-{id}` 文件中
   - 使用 JSON 格式序列化 Schema 信息

2. **数据存储**：
   - 数据通过 LSM Tree 存储引擎写入 SSTable 文件
   - 文件路径：`warehouse/{database}/{table}/data/data-{snapshot}-{id}.sst`

3. **快照管理**：
   - 每次数据写入都会创建新的快照
   - 快照信息存储在 `warehouse/{database}/{table}/snapshot/` 目录中
   - Manifest 文件跟踪数据变更

## 文件结构

```
warehouse/
├── {database}/
│   └── {table}/
│       ├── schema/
│       │   └── schema-0              # 表结构定义
│       ├── data/
│       │   └── data-0-000.sst        # 数据文件
│       ├── snapshot/
│       │   ├── snapshot-0            # 快照信息
│       │   └── LATEST                # 最新快照指针
│       └── manifest/
│           ├── manifest-list-0       # Manifest 文件列表
│           └── manifest-*            # 具体的 Manifest 文件
```

## 运行示例

```bash
# 编译项目
mvn clean compile

# 运行 SQL 演示
java -cp "target/classes:lib/*" com.minipaimon.sql.SQLDemo
```

## 测试验证

运行 SQLDemo 后，可以验证以下内容：

1. 表结构已正确创建并持久化
2. 数据已成功写入 SSTable 文件
3. 快照和 Manifest 文件已生成
4. 不同数据类型（包括 DOUBLE）得到正确处理

## 限制和未来改进

当前实现的限制：
1. SQL 解析器较为简单，不支持复杂表达式
2. 不支持 SELECT 查询语句
3. 不支持表的修改和删除操作
4. 不支持事务和并发控制

未来可以改进的方向：
1. 集成更强大的 SQL 解析器（如 Apache Calcite）
2. 实现 SELECT 查询功能
3. 添加索引支持
4. 实现更完善的错误处理和事务支持