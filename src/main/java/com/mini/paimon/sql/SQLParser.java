package com.mini.paimon.sql;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.TableManager;
import com.mini.paimon.storage.LSMTree;
import com.mini.paimon.table.DataTableRead;
import com.mini.paimon.table.DataTableScan;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.metadata.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * SQL 解析器
 * 支持 CREATE TABLE、INSERT 和 SELECT 语句的解析
 * 使用 Druid SQL Parser 实现标准 SQL 解析
 */
public class SQLParser {
    
    private final TableManager tableManager;
    private final PathFactory pathFactory;
    
    public SQLParser(TableManager tableManager, PathFactory pathFactory) {
        this.tableManager = tableManager;
        this.pathFactory = pathFactory;
    }
    
    /**
     * 解析并执行 SQL 语句
     * 
     * @param sql SQL 语句
     * @throws IOException 解析或执行错误
     */
    public void executeSQL(String sql) throws IOException {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL);
        List<SQLStatement> statements = parser.parseStatementList();
        
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("Empty SQL statement");
        }
        
        SQLStatement statement = statements.get(0);
        
        if (statement instanceof SQLCreateTableStatement) {
            executeCreateTable((SQLCreateTableStatement) statement);
        } else if (statement instanceof SQLInsertStatement) {
            executeInsert((SQLInsertStatement) statement);
        } else if (statement instanceof SQLSelectStatement) {
            executeSelect((SQLSelectStatement) statement);
        } else {
            throw new IllegalArgumentException("Unsupported SQL statement: " + statement.getClass().getSimpleName());
        }
    }
    
    /**
     * 解析并执行 CREATE TABLE 语句
     * 
     * @param stmt CREATE TABLE 语句
     * @throws IOException 解析或执行错误
     */
    private void executeCreateTable(SQLCreateTableStatement stmt) throws IOException {
        String tableName = stmt.getTableSource().toString();
        
        List<Field> fields = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        
        // 解析列定义
        for (SQLTableElement element : stmt.getTableElementList()) {
            if (element instanceof SQLColumnDefinition) {
                SQLColumnDefinition columnDef = (SQLColumnDefinition) element;
                String columnName = columnDef.getName().getSimpleName();
                
                // 获取数据类型
                SQLDataType sqlDataType = columnDef.getDataType();
                DataType dataType = convertDataType(sqlDataType);
                
                // 检查是否为可空字段
                boolean nullable = true;
                for (SQLColumnConstraint constraint : columnDef.getConstraints()) {
                    if (constraint instanceof SQLNotNullConstraint) {
                        nullable = false;
                        break;
                    }
                }
                
                fields.add(new Field(columnName, dataType, nullable));
            }
        }
        
        // 检查是否有在列定义中指定的主键约束（向后兼容）
        for (SQLTableElement element : stmt.getTableElementList()) {
            if (element instanceof SQLColumnDefinition) {
                SQLColumnDefinition columnDef = (SQLColumnDefinition) element;
                for (SQLColumnConstraint constraint : columnDef.getConstraints()) {
                    if ("PRIMARY KEY".equalsIgnoreCase(constraint.getClass().getSimpleName())) {
                        primaryKeys.add(columnDef.getName().getSimpleName());
                        break;
                    }
                }
            }
        }
        
        // 如果没有显式指定主键，但有 NOT NULL 字段，可以将其作为主键（简化处理）
        if (primaryKeys.isEmpty()) {
            for (Field field : fields) {
                if (!field.isNullable()) {
                    primaryKeys.add(field.getName());
                    break;
                }
            }
        }
        
        // 创建表
        tableManager.createTable("default", tableName, fields, primaryKeys, new ArrayList<>());
        
        System.out.println("Table '" + tableName + "' created successfully.");
    }
    
    /**
     * 解析并执行 INSERT 语句
     * 
     * @param stmt INSERT 语句
     * @throws IOException 解析或执行错误
     */
    private void executeInsert(SQLInsertStatement stmt) throws IOException {
        String tableName = stmt.getTableName().getSimpleName();
        
        // 获取表的 Schema
        Schema schema = tableManager.getSchemaManager("default", tableName)
                                   .getCurrentSchema();
        
        if (schema == null) {
            throw new IOException("Table '" + tableName + "' not found");
        }
        
        List<Field> fields = schema.getFields();
        
        // 处理列名（如果指定了）
        List<String> columnNames = new ArrayList<>();
        if (stmt.getColumns().isEmpty()) {
            // 使用所有列
            for (Field field : fields) {
                columnNames.add(field.getName());
            }
        } else {
            // 使用指定列
            for (SQLExpr columnExpr : stmt.getColumns()) {
                if (columnExpr instanceof SQLIdentifierExpr) {
                    columnNames.add(((SQLIdentifierExpr) columnExpr).getName());
                } else {
                    throw new IllegalArgumentException("Unsupported column expression: " + columnExpr);
                }
            }
        }
        
        // 处理值
        List<SQLInsertStatement.ValuesClause> valuesList = stmt.getValuesList();
        if (valuesList.isEmpty()) {
            throw new IllegalArgumentException("No values specified in INSERT statement");
        }
        
        // 只处理第一行数据（简化处理）
        SQLInsertStatement.ValuesClause valuesClause = valuesList.get(0);
        List<SQLExpr> valueExprs = valuesClause.getValues();
        
        // 验证列数和值数是否匹配
        if (valueExprs.size() != columnNames.size()) {
            throw new IllegalArgumentException(
                "Value count doesn't match column count. Expected: " + columnNames.size() + ", Actual: " + valueExprs.size());
        }
        
        // 构造数据行
        Object[] rowValues = new Object[fields.size()];
        
        // 按指定列名填充
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int columnIndex = columnNames.indexOf(field.getName());
            if (columnIndex >= 0) {
                SQLExpr valueExpr = valueExprs.get(columnIndex);
                rowValues[i] = convertValue(valueExpr, field.getType());
            } else {
                // 未指定的列使用默认值或 null
                rowValues[i] = null;
            }
        }
        
        // 创建 LSMTree 并插入数据
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "default", tableName);
        Row row = new Row(rowValues);
        lsmTree.put(row);
        lsmTree.close(); // 关闭以触发刷写
        
        System.out.println("Data inserted into table '" + tableName + "' successfully.");
    }
    
    /**
     * 解析并执行 SELECT 语句
     * 支持字段投影和 WHERE 条件过滤
     * 
     * @param stmt SELECT 语句
     * @throws IOException 解析或执行错误
     */
    private void executeSelect(SQLSelectStatement stmt) throws IOException {
        SQLSelectQuery query = stmt.getSelect().getQuery();
        
        if (!(query instanceof SQLSelectQueryBlock)) {
            throw new IllegalArgumentException("Only simple SELECT queries are supported");
        }
        
        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
        
        // 1. 获取表名
        if (!(queryBlock.getFrom() instanceof SQLExprTableSource)) {
            throw new IllegalArgumentException("Only single table SELECT is supported");
        }
        
        SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.getFrom();
        String tableName = tableSource.getExpr().toString();
        
        // 2. 获取表的 Schema
        Schema schema = tableManager.getSchemaManager("default", tableName)
                                   .getCurrentSchema();
        
        if (schema == null) {
            throw new IOException("Table '" + tableName + "' not found");
        }
        
        // 3. 解析投影（SELECT 字段列表）
        Projection projection = parseProjection(queryBlock, schema);
        
        // 4. 解析 WHERE 条件
        Predicate predicate = parseWhere(queryBlock, schema);
        
        // 5. 执行查询：DataTableScan -> DataTableRead
        DataTableScan tableScan = new DataTableScan(pathFactory, "default", tableName, schema);
        DataTableScan.Plan plan = tableScan.plan();
        
        DataTableRead tableRead = new DataTableRead(schema);
        if (projection != null) {
            tableRead.withProjection(projection);
        }
        if (predicate != null) {
            tableRead.withFilter(predicate);
        }
        
        List<Row> rows = tableRead.read(plan);
        
        // 6. 打印结果
        Schema resultSchema = projection != null ? projection.projectSchema(schema) : schema;
        printResults(resultSchema, rows);
    }
    
    /**
     * 打印查询结果
     * 
     * @param schema 表的Schema
     * @param rows 查询到的数据行
     */
    private void printResults(Schema schema, List<Row> rows) {
        // 打印列标题
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) System.out.print("\t");
            System.out.print(fields.get(i).getName());
        }
        System.out.println();
        
        // 打印分隔线
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) System.out.print("\t");
            System.out.print("--------");
        }
        System.out.println();
        
        // 打印数据行
        for (Row row : rows) {
            Object[] values = row.getValues();
            for (int i = 0; i < values.length; i++) {
                if (i > 0) System.out.print("\t");
                System.out.print(values[i] != null ? values[i].toString() : "NULL");
            }
            System.out.println();
        }
        
        System.out.println(rows.size() + " row(s) returned");
    }
    
    /**
     * 转换 SQL 数据类型为内部数据类型
     */
    private DataType convertDataType(SQLDataType sqlDataType) {
        String typeName = sqlDataType.getName().toUpperCase();
        
        switch (typeName) {
            case "INT":
            case "INTEGER":
                return DataType.INT;
            case "BIGINT":
            case "LONG":
                return DataType.LONG;
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
            case "STRING":
                return DataType.STRING;
            case "BOOLEAN":
            case "BOOL":
                return DataType.BOOLEAN;
            case "DOUBLE":
            case "FLOAT":
                return DataType.DOUBLE;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + typeName);
        }
    }
    
    /**
     * 转换 SQL 表达式值为指定类型
     */
    private Object convertValue(SQLExpr valueExpr, DataType dataType) {
        if (valueExpr instanceof SQLNullExpr) {
            return null;
        }
        
        String valueStr;
        if (valueExpr instanceof SQLCharExpr) {
            valueStr = ((SQLCharExpr) valueExpr).getText();
        } else if (valueExpr instanceof SQLIntegerExpr) {
            valueStr = String.valueOf(((SQLIntegerExpr) valueExpr).getNumber());
        } else if (valueExpr instanceof SQLBooleanExpr) {
            valueStr = String.valueOf(((SQLBooleanExpr) valueExpr).getValue());
        } else if (valueExpr instanceof SQLNumberExpr) {
            valueStr = String.valueOf(((SQLNumberExpr) valueExpr).getNumber());
        } else {
            valueStr = valueExpr.toString();
            // 移除可能的引号
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                valueStr = valueStr.substring(1, valueStr.length() - 1);
            } else if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
                valueStr = valueStr.substring(1, valueStr.length() - 1);
            }
        }
        
        switch (dataType) {
            case INT:
                return Integer.parseInt(valueStr);
            case LONG:
                return Long.parseLong(valueStr);
            case STRING:
                return valueStr;
            case BOOLEAN:
                return Boolean.parseBoolean(valueStr);
            case DOUBLE:
                return Double.parseDouble(valueStr);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
    
    /**
     * 解析投影（SELECT 字段列表）
     */
    private Projection parseProjection(SQLSelectQueryBlock queryBlock, Schema schema) {
        List<SQLSelectItem> selectItems = queryBlock.getSelectList();
        
        // 检查是否是 SELECT *
        if (selectItems.size() == 1 && selectItems.get(0).getExpr() instanceof SQLAllColumnExpr) {
            return Projection.all();
        }
        
        // 解析具体字段
        List<String> fields = new ArrayList<>();
        for (SQLSelectItem item : selectItems) {
            SQLExpr expr = item.getExpr();
            
            if (expr instanceof SQLIdentifierExpr) {
                fields.add(((SQLIdentifierExpr) expr).getName());
            } else if (expr instanceof SQLPropertyExpr) {
                // 支持 table.column 形式
                fields.add(((SQLPropertyExpr) expr).getName());
            } else {
                throw new IllegalArgumentException("Unsupported SELECT expression: " + expr);
            }
        }
        
        return Projection.of(fields);
    }
    
    /**
     * 解析 WHERE 条件
     */
    private Predicate parseWhere(SQLSelectQueryBlock queryBlock, Schema schema) {
        SQLExpr whereExpr = queryBlock.getWhere();
        if (whereExpr == null) {
            return null;
        }
        
        return parseExpression(whereExpr, schema);
    }
    
    /**
     * 解析表达式为谓词
     */
    private Predicate parseExpression(SQLExpr expr, Schema schema) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binaryExpr = (SQLBinaryOpExpr) expr;
            
            // 逻辑运算符
            if (binaryExpr.getOperator() == SQLBinaryOperator.BooleanAnd) {
                Predicate left = parseExpression(binaryExpr.getLeft(), schema);
                Predicate right = parseExpression(binaryExpr.getRight(), schema);
                return left.and(right);
            } else if (binaryExpr.getOperator() == SQLBinaryOperator.BooleanOr) {
                Predicate left = parseExpression(binaryExpr.getLeft(), schema);
                Predicate right = parseExpression(binaryExpr.getRight(), schema);
                return left.or(right);
            }
            
            // 比较运算符
            String fieldName = extractFieldName(binaryExpr.getLeft());
            Object value = extractValue(binaryExpr.getRight(), schema, fieldName);
            
            switch (binaryExpr.getOperator()) {
                case Equality:
                    return Predicate.equal(fieldName, value);
                case NotEqual:
                case LessThanOrGreater:
                    return Predicate.notEqual(fieldName, value);
                case GreaterThan:
                    return Predicate.greaterThan(fieldName, value);
                case GreaterThanOrEqual:
                    return Predicate.greaterOrEqual(fieldName, value);
                case LessThan:
                    return Predicate.lessThan(fieldName, value);
                case LessThanOrEqual:
                    return Predicate.lessOrEqual(fieldName, value);
                default:
                    throw new IllegalArgumentException("Unsupported operator: " + binaryExpr.getOperator());
            }
        }
        
        throw new IllegalArgumentException("Unsupported WHERE expression: " + expr);
    }
    
    /**
     * 提取字段名
     */
    private String extractFieldName(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) expr).getName();
        } else if (expr instanceof SQLPropertyExpr) {
            return ((SQLPropertyExpr) expr).getName();
        }
        throw new IllegalArgumentException("Unsupported field expression: " + expr);
    }
    
    /**
     * 提取值
     */
    private Object extractValue(SQLExpr expr, Schema schema, String fieldName) {
        // 查找字段类型
        DataType dataType = null;
        for (Field field : schema.getFields()) {
            if (field.getName().equals(fieldName)) {
                dataType = field.getType();
                break;
            }
        }
        
        if (dataType == null) {
            throw new IllegalArgumentException("Field not found: " + fieldName);
        }
        
        return convertValue(expr, dataType);
    }
}