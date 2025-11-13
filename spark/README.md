# Spark 模块

Mini-Paimon 的 Spark 集成模块，提供 Spark SQL 访问 Paimon 表的能力。

## 架构设计

### 核心组件

```
spark/
├── catalog/              # Spark Catalog 集成
│   ├── SparkCatalog.java             # Spark Catalog 实现
│   ├── SparkSchemaConverter.java     # Schema 转换器
│   └── SparkTable.java               # Table 实现
├── source/               # 数据源
│   ├── PaimonBatch.java              # 批处理读取
│   ├── PaimonInputPartition.java     # 分区定义
│   ├── PaimonPartitionReader.java    # 分区读取器
│   └── PaimonPartitionReaderFactory.java
├── sink/                 # 数据写入
│   ├── PaimonBatchWrite.java         # 批处理写入
│   ├── PaimonDataWriter.java         # 数据写入器
│   ├── PaimonDataWriterFactory.java
│   └── PaimonWriterCommitMessage.java
├── table/                # 表读写
│   └── SparkRowConverter.java        # 行数据转换
└── utils/                # 工具类
    └── SparkSessionFactory.java      # Session 工厂
```

### 设计原则

1. **严格遵循 Apache Paimon 风格**
   - 类命名与 Paimon 官方保持一致
   - 接口设计参考 Paimon Spark 模块
   - 保持代码简洁

2. **与 Paimon 核心模块解耦**
   - 通过 Catalog 接口访问元数据
   - 使用 Table 接口进行数据读写
   - 不直接依赖 LSMTree 等底层实现

3. **支持标准 Spark SQL**
   - CREATE/DROP DATABASE
   - CREATE/DROP TABLE
   - INSERT/SELECT 操作
   - 分区表支持

## 快速开始

### 1. 编译项目

```bash
mvn clean compile -pl spark -am
```

### 2. 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.mini.paimon.spark.QuickStartExample" -pl spark
```

### 3. 代码示例

```java
import com.mini.paimon.spark.utils.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

// 创建 Spark Session
SparkSession spark = SparkSessionFactory.createSparkSession("warehouse");

// 创建数据库
spark.sql("CREATE DATABASE IF NOT EXISTS paimon.test_db").show();
spark.sql("USE paimon.test_db").show();

// 创建表
spark.sql(
    "CREATE TABLE users (" +
    "  id BIGINT," +
    "  name STRING," +
    "  age INT" +
    ") USING paimon"
).show();

// 插入数据
spark.sql(
    "INSERT INTO users VALUES " +
    "(1, 'Alice', 25), " +
    "(2, 'Bob', 30)"
).show();

// 查询数据
spark.sql("SELECT * FROM users").show();
```

## 支持的功能

### DDL 操作

- ✅ CREATE DATABASE
- ✅ DROP DATABASE
- ✅ CREATE TABLE
- ✅ DROP TABLE
- ✅ 分区表支持

### DML 操作

- ✅ INSERT INTO
- ✅ SELECT 查询
- ✅ WHERE 过滤
- ✅ JOIN 操作
- ✅ GROUP BY 聚合
- ✅ ORDER BY 排序
- ⏳ UPDATE (计划中)
- ⏳ DELETE (计划中)

### 数据类型

- ✅ INT
- ✅ BIGINT (映射为 LONG)
- ✅ STRING
- ✅ BOOLEAN
- ✅ DOUBLE
- ⏳ TIMESTAMP (计划中)
- ⏳ DATE (计划中)

## 配置选项

### Catalog 配置

在创建 SparkSession 时配置：

```java
SparkSession spark = SparkSession.builder()
    .appName("mini-paimon")
    .master("local[*]")
    .config("spark.sql.catalog.paimon", "com.mini.paimon.spark.catalog.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", "/path/to/warehouse")
    .getOrCreate();
```

### 表选项

```sql
CREATE TABLE table_name (
  ...
) USING paimon
TBLPROPERTIES (
  'primary-key' = 'id',
  'path' = '/custom/path'
)
```

## 与 Apache Paimon 的差异

| 功能 | Mini-Paimon | Apache Paimon |
|------|-------------|---------------|
| 基础 DDL | ✅ | ✅ |
| 基础 DML | ✅ | ✅ |
| 流式读取 | ⏳ | ✅ |
| CDC 集成 | ⏳ | ✅ |
| Time Travel | ⏳ | ✅ |
| 数据类型 | 5种 | 20+ 种 |

## 开发指南

### 添加新数据类型

1. 在 `DataType` 枚举中添加类型
2. 更新 `SparkSchemaConverter.toDataType()`
3. 更新 `SparkSchemaConverter.toSparkType()`
4. 更新 `SparkRowConverter` 转换逻辑

### 添加新操作

1. 在对应的 Catalog 方法中实现
2. 确保与 Paimon 核心模块 API 一致
3. 添加测试用例

## 运行测试

### 基础测试

```bash
mvn test -pl spark -Dtest=SparkSQLExample
```

### 集成测试

```bash
mvn test -pl spark -Dtest=SparkPaimonIntegrationTest
```

## 注意事项

1. **编码规范**：遵循 Paimon 风格，少注释多代码
2. **错误处理**：不合理代码直接删除重写，不要叠加修改
3. **接口兼容**：保持与 Spark DataSource V2 API 的兼容性
4. **性能优化**：避免不必要的数据转换和复制

## 未来计划

- [ ] 支持流式读取
- [ ] 支持 UPDATE/DELETE 操作
- [ ] 支持更多数据类型
- [ ] 支持 Time Travel 查询
- [ ] 性能优化

## 参考资料

- [Apache Paimon 官方文档](https://paimon.apache.org/)
- [Apache Spark SQL](https://spark.apache.org/sql/)
- [Spark DataSource V2 API](https://spark.apache.org/docs/latest/sql-data-sources-v2.html)

