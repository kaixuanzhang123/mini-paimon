package com.minipaimon.sql;

import com.minipaimon.metadata.DataType;
import com.minipaimon.metadata.Field;
import com.minipaimon.metadata.Schema;
import com.minipaimon.metadata.TableManager;
import com.minipaimon.storage.LSMTree;
import com.minipaimon.utils.PathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL 解析器
 * 支持 CREATE TABLE 和 INSERT 语句的解析
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
        String trimmedSql = sql.trim();
        if (trimmedSql.toUpperCase().startsWith("CREATE TABLE")) {
            executeCreateTable(trimmedSql);
        } else if (trimmedSql.toUpperCase().startsWith("INSERT INTO")) {
            executeInsert(trimmedSql);
        } else {
            throw new IllegalArgumentException("Unsupported SQL statement: " + trimmedSql);
        }
    }
    
    /**
     * 解析并执行 CREATE TABLE 语句
     * 
     * @param sql CREATE TABLE 语句
     * @throws IOException 解析或执行错误
     */
    private void executeCreateTable(String sql) throws IOException {
        // 简化的 CREATE TABLE 语法解析
        // CREATE TABLE table_name (column1 type1 PRIMARY KEY, column2 type2, ...)
        
        // 提取表名
        Pattern tableNamePattern = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher tableNameMatcher = tableNamePattern.matcher(sql);
        if (!tableNameMatcher.find()) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax: " + sql);
        }
        String tableName = tableNameMatcher.group(1);
        
        // 提取列定义部分
        int startParen = sql.indexOf('(');
        int endParen = sql.lastIndexOf(')');
        if (startParen == -1 || endParen == -1 || endParen <= startParen) {
            throw new IllegalArgumentException("Invalid CREATE TABLE syntax: " + sql);
        }
        
        String columnsDef = sql.substring(startParen + 1, endParen);
        
        // 解析列定义
        List<Field> fields = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        
        String[] columnDefs = columnsDef.split(",");
        for (String columnDef : columnDefs) {
            columnDef = columnDef.trim();
            if (columnDef.isEmpty()) continue;
            
            // 解析列定义
            String[] parts = columnDef.split("\\s+");
            if (parts.length < 2) {
                throw new IllegalArgumentException("Invalid column definition: " + columnDef);
            }
            
            String columnName = parts[0];
            String columnType = parts[1].toUpperCase();
            
            // 检查是否为主键
            boolean isPrimaryKey = false;
            for (int i = 2; i < parts.length; i++) {
                if ("PRIMARY".equalsIgnoreCase(parts[i]) && i + 1 < parts.length && 
                    "KEY".equalsIgnoreCase(parts[i + 1])) {
                    isPrimaryKey = true;
                    break;
                }
            }
            
            // 跳过 PRIMARY KEY 关键字本身
            if ("PRIMARY".equalsIgnoreCase(columnName)) {
                continue;
            }
            
            DataType dataType;
            switch (columnType) {
                case "INT":
                    dataType = DataType.INT;
                    break;
                case "BIGINT":
                case "LONG":
                    dataType = DataType.LONG;
                    break;
                case "STRING":
                case "VARCHAR":
                case "TEXT":
                    dataType = DataType.STRING;
                    break;
                case "BOOLEAN":
                case "BOOL":
                    dataType = DataType.BOOLEAN;
                    break;
                case "DOUBLE":
                case "FLOAT":
                    dataType = DataType.DOUBLE;
                    break;
                default:
                    // 跳过不支持的类型
                    continue;
            }
            
            // 检查是否为可空字段（简化处理）
            boolean nullable = true;
            for (int i = 2; i < parts.length; i++) {
                if ("NOT".equalsIgnoreCase(parts[i]) && i + 1 < parts.length && 
                    "NULL".equalsIgnoreCase(parts[i + 1])) {
                    nullable = false;
                    break;
                }
            }
            
            fields.add(new Field(columnName, dataType, nullable));
            
            // 如果是主键，添加到主键列表
            if (isPrimaryKey) {
                primaryKeys.add(columnName);
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
        Schema schema = new Schema(0, fields, primaryKeys);
        tableManager.createTable("default", tableName, schema.getFields(), schema.getPrimaryKeys(), new ArrayList<>());
        
        System.out.println("Table '" + tableName + "' created successfully.");
    }
    
    /**
     * 解析并执行 INSERT 语句
     * 
     * @param sql INSERT 语句
     * @throws IOException 解析或执行错误
     */
    private void executeInsert(String sql) throws IOException {
        // 简化的 INSERT 语法解析
        // INSERT INTO table_name VALUES (value1, value2, ...)
        // INSERT INTO table_name (col1, col2, ...) VALUES (value1, value2, ...)
        
        Pattern pattern1 = Pattern.compile(
            "INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\((.+)\\)",
            Pattern.CASE_INSENSITIVE
        );
        
        Pattern pattern2 = Pattern.compile(
            "INSERT\\s+INTO\\s+(\\w+)\\s*\\((.+)\\)\\s+VALUES\\s*\\((.+)\\)",
            Pattern.CASE_INSENSITIVE
        );
        
        Matcher matcher1 = pattern1.matcher(sql);
        Matcher matcher2 = pattern2.matcher(sql);
        
        String tableName;
        List<String> columnNames;
        List<String> values;
        
        if (matcher1.matches()) {
            // INSERT INTO table_name VALUES (value1, value2, ...)
            tableName = matcher1.group(1);
            columnNames = null; // 使用所有列
            values = parseValues(matcher1.group(2));
        } else if (matcher2.matches()) {
            // INSERT INTO table_name (col1, col2, ...) VALUES (value1, value2, ...)
            tableName = matcher2.group(1);
            columnNames = parseColumnNames(matcher2.group(2));
            values = parseValues(matcher2.group(3));
        } else {
            throw new IllegalArgumentException("Invalid INSERT syntax: " + sql);
        }
        
        // 获取表的 Schema
        Schema schema = tableManager.getSchemaManager("default", tableName)
                                   .getCurrentSchema();
        
        if (schema == null) {
            throw new IOException("Table '" + tableName + "' not found");
        }
        
        // 验证列数和值数是否匹配
        List<Field> fields = schema.getFields();
        if (columnNames == null) {
            // 使用所有列
            if (values.size() != fields.size()) {
                throw new IllegalArgumentException(
                    "Value count doesn't match column count. Expected: " + fields.size() + ", Actual: " + values.size());
            }
        } else {
            // 使用指定列
            if (values.size() != columnNames.size()) {
                throw new IllegalArgumentException(
                    "Value count doesn't match column count. Expected: " + columnNames.size() + ", Actual: " + values.size());
            }
        }
        
        // 构造数据行
        Object[] rowValues = new Object[fields.size()];
        
        if (columnNames == null) {
            // 按顺序填充所有列
            for (int i = 0; i < fields.size(); i++) {
                rowValues[i] = convertValue(values.get(i), fields.get(i).getType());
            }
        } else {
            // 按指定列名填充
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                int columnIndex = columnNames.indexOf(field.getName());
                if (columnIndex >= 0) {
                    rowValues[i] = convertValue(values.get(columnIndex), field.getType());
                } else {
                    // 未指定的列使用默认值或 null
                    rowValues[i] = null;
                }
            }
        }
        
        // 创建 LSMTree 并插入数据
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "default", tableName);
        com.minipaimon.metadata.Row row = new com.minipaimon.metadata.Row(rowValues);
        lsmTree.put(row);
        lsmTree.close(); // 关闭以触发刷写
        
        System.out.println("Data inserted into table '" + tableName + "' successfully.");
    }
    
    /**
     * 解析列名列表
     */
    private List<String> parseColumnNames(String columnNamesStr) {
        List<String> columnNames = new ArrayList<>();
        String[] names = columnNamesStr.split(",");
        for (String name : names) {
            columnNames.add(name.trim());
        }
        return columnNames;
    }
    
    /**
     * 解析值列表
     */
    private List<String> parseValues(String valuesStr) {
        List<String> values = new ArrayList<>();
        // 简化处理，按逗号分割（不处理字符串中的逗号）
        String[] valueArray = valuesStr.split(",");
        for (String value : valueArray) {
            values.add(value.trim());
        }
        return values;
    }
    
    /**
     * 转换字符串值为指定类型
     */
    private Object convertValue(String valueStr, DataType dataType) {
        // 移除可能的引号
        if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
            valueStr = valueStr.substring(1, valueStr.length() - 1);
        } else if (valueStr.startsWith("\"") && valueStr.endsWith("\"")) {
            valueStr = valueStr.substring(1, valueStr.length() - 1);
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
}