# Flink 模块

Mini-Paimon 的 Flink 集成模块，提供 Flink SQL 访问 Paimon 表的能力。

## 架构设计

### 核心组件

```
flink/
├── catalog/              # Flink Catalog 集成
│   ├── FlinkCatalog.java             # Flink Catalog 实现
│   ├── FlinkSchemaConverter.java     # Schema 转换器
│   └── FlinkTableFactory.java        # Table 工厂
├── table/                # 表读写
│   ├── FlinkTableWriter.java         # 写入器
│   └── FlinkRowConverter.java        # 行数据转换
└── utils/                # 工具类
    └── FlinkEnvironmentFactory.java  # 环境工厂
```

### 设计原则

1. **严格遵循 Apache Paimon 风格**
   - 类命名与 Paimon 官方保持一致
   - 接口设计参考 Paimon Flink 模块
   - 保持代码简洁，注释精简

2. **与 Paimon 核心模块解耦**
   - 通过 Catalog 接口访问元数据
   - 使用 Table 接口进行数据读写
   - 不直接依赖 LSMTree 等底层实现

3. **支持标准 Flink SQL**
   - CREATE/DROP DATABASE
   - CREATE/DROP TABLE
   - INSERT/SELECT 操作
   - 分区表支持

## 快速开始

### 1. 编译项目

```bash
mvn clean compile -pl flink -am
```

### 2. 运行示例

```bash
mvn exec:java -Dexec.mainClass="com.mini.paimon.flink.FlinkSQLExample" -pl flink
```

### 3. 代码示例

```java
import com.mini.paimon.flink.utils.FlinkEnvironmentFactory;
import org.apache.flink.table.api.TableEnvironment;

// 创建 Flink Table 环境
TableEnvironment tableEnv = FlinkEnvironmentFactory.createTableEnvironment("warehouse");

// 创建数据库
tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db");
tableEnv.executeSql("USE test_db");

// 创建表
tableEnv.executeSql(
    "CREATE TABLE users (" +
    "  id BIGINT," +
    "  name STRING," +
    "  age INT," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH ('connector' = 'mini-paimon')"
);

// 插入数据
tableEnv.executeSql(
    "INSERT INTO users VALUES " +
    "(1, 'Alice', 25), " +
    "(2, 'Bob', 30)"
).await();

// 查询数据
TableResult result = tableEnv.executeSql("SELECT * FROM users");
result.print();
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

## 运行测试

### 基础测试

```bash
mvn test -pl flink -Dtest=FlinkSQLExample
```

### 集成测试

```bash
mvn test -pl flink -Dtest=FlinkPaimonIntegrationTest
```

## 配置选项

### Catalog 配置

```java
CatalogContext context = CatalogContext.builder()
    .warehouse("/path/to/warehouse")
    .option("key", "value")
    .build();
```

### 表选项

```sql
CREATE TABLE table_name (
  ...
) WITH (
  'connector' = 'mini-paimon',
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
| Lookup Join | ⏳ | ✅ |
| Time Travel | ⏳ | ✅ |
| 数据类型 | 5种 | 20+ 种 |

## 开发指南

### 添加新数据类型

1. 在 `DataType` 枚举中添加类型
2. 更新 `FlinkSchemaConverter.toDataType()`
3. 更新 `FlinkSchemaConverter.toFlinkType()`
4. 更新 `FlinkRowConverter` 转换逻辑

### 添加新操作

1. 在对应的 Catalog 方法中实现
2. 确保与 Paimon 核心模块 API 一致
3. 添加测试用例

## 注意事项

1. **编码规范**：遵循 Paimon 风格，少注释多代码
2. **错误处理**：不合理代码直接删除重写，不要叠加修改
3. **接口兼容**：保持与 Flink Table API 的兼容性
4. **性能优化**：避免不必要的数据转换和复制

## 未来计划

- [ ] 支持流式读取
- [ ] 支持 UPDATE/DELETE 操作
- [ ] 支持更多数据类型
- [ ] 支持 Lookup Join
- [ ] 支持 Time Travel 查询
- [ ] 性能优化

## 参考资料

- [Apache Paimon 官方文档](https://paimon.apache.org/)
- [Apache Flink Table API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/)
