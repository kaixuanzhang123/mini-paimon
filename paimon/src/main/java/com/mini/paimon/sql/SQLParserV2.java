package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.index.IndexType;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.sql.parser.MiniPaimonSQLBaseVisitor;
import com.mini.paimon.sql.parser.MiniPaimonSQLLexer;
import com.mini.paimon.sql.parser.MiniPaimonSQLParser;
import com.mini.paimon.table.*;
import com.mini.paimon.utils.PathFactory;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 基于 ANTLR4 的 SQL 解析器（工业级实现）
 * 完整支持 TBLPROPERTIES 语法，类似 Spark SQL
 */
public class SQLParserV2 {
    private static final Logger logger = LoggerFactory.getLogger(SQLParserV2.class);
    
    private final Catalog catalog;
    private final PathFactory pathFactory;
    
    public SQLParserV2(Catalog catalog, PathFactory pathFactory) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
    }
    
    /**
     * 执行 SQL 语句
     */
    public void executeSQL(String sql) throws IOException {
        try {
            // 词法分析
            MiniPaimonSQLLexer lexer = new MiniPaimonSQLLexer(CharStreams.fromString(sql));
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            
            // 语法分析
            MiniPaimonSQLParser parser = new MiniPaimonSQLParser(tokens);
            parser.removeErrorListeners(); // 移除默认错误监听器
            parser.addErrorListener(new SQLErrorListener()); // 添加自定义错误监听器
            
            ParseTree tree = parser.statement();
            
            // 语义分析和执行
            SQLExecutor executor = new SQLExecutor(catalog, pathFactory);
            executor.visit(tree);
            
        } catch (Exception e) {
            throw new IOException("Failed to execute SQL: " + e.getMessage(), e);
        }
    }
    
    /**
     * SQL 执行器（访问者模式）
     */
    private static class SQLExecutor extends MiniPaimonSQLBaseVisitor<Void> {
        private final Catalog catalog;
        private final PathFactory pathFactory;
        
        public SQLExecutor(Catalog catalog, PathFactory pathFactory) {
            this.catalog = catalog;
            this.pathFactory = pathFactory;
        }
        
        @Override
        public Void visitCreateTableStatement(MiniPaimonSQLParser.CreateTableStatementContext ctx) {
            try {
                // 解析表名
                String tableName = ctx.qualifiedName().getText();
                Identifier identifier = parseIdentifier(tableName);
                
                // 解析字段
                List<Field> fields = new ArrayList<>();
                List<String> primaryKeys = new ArrayList<>();
                
                for (MiniPaimonSQLParser.ColumnDefinitionContext colDef : ctx.columnDefinition()) {
                    String columnName = colDef.identifier().getText();
                    DataType dataType = parseDataType(colDef.dataType());
                    boolean nullable = colDef.NOT() == null;
                    
                    fields.add(new Field(columnName, dataType, nullable));
                    
                    // 如果是 NOT NULL，加入主键候选
                    if (!nullable) {
                        primaryKeys.add(columnName);
                    }
                }
                
                // 解析分区键
                List<String> partitionKeys = new ArrayList<>();
                if (ctx.PARTITIONED() != null) {
                    for (MiniPaimonSQLParser.IdentifierContext id : ctx.identifier()) {
                        partitionKeys.add(id.getText());
                    }
                }
                
                // 解析 TBLPROPERTIES
                Map<String, List<IndexType>> indexConfig = new HashMap<>();
                if (ctx.TBLPROPERTIES() != null) {
                    Map<String, String> properties = new HashMap<>();
                    for (MiniPaimonSQLParser.TablePropertyContext prop : ctx.tableProperty()) {
                        String key = unquote(prop.STRING_LITERAL(0).getText());
                        String value = unquote(prop.STRING_LITERAL(1).getText());
                        properties.put(key, value);
                    }
                    indexConfig = parseIndexConfig(properties, fields);
                }
                
                // 创建 Schema
                Schema schema = new Schema(-1, fields, primaryKeys, partitionKeys);
                
                // 创建表
                catalog.createTableWithIndex(identifier, schema, indexConfig, true);
                
                System.out.println("Table '" + identifier.getFullName() + "' created successfully.");
                if (!partitionKeys.isEmpty()) {
                    System.out.println("Partition keys: " + partitionKeys);
                }
                if (!indexConfig.isEmpty()) {
                    System.out.println("Index configuration: " + formatIndexConfig(indexConfig));
                }
                
            } catch (Exception e) {
                logger.error("Failed to execute CREATE TABLE", e);
                throw new RuntimeException("Failed to execute CREATE TABLE: " + e.getMessage(), e);
            }
            return null;
        }
        
        @Override
        public Void visitInsertStatement(MiniPaimonSQLParser.InsertStatementContext ctx) {
            try {
                String tableName = ctx.qualifiedName().getText();
                Identifier identifier = parseIdentifier(tableName);
                
                Schema schema = catalog.getTableSchema(identifier);
                if (schema == null) {
                    throw new IOException("Table not found: " + identifier.getFullName());
                }
                
                // 解析列名
                List<String> columnNames = new ArrayList<>();
                if (ctx.identifier() != null && !ctx.identifier().isEmpty()) {
                    for (MiniPaimonSQLParser.IdentifierContext id : ctx.identifier()) {
                        columnNames.add(id.getText());
                    }
                } else {
                    for (Field field : schema.getFields()) {
                        columnNames.add(field.getName());
                    }
                }
                
                // 插入数据
                Table table = catalog.getTable(identifier);
                try (TableWrite writer = table.newWrite()) {
                    // 遍历所有值组
                    int rowCount = 0;
                    List<MiniPaimonSQLParser.ValuesRowContext> valuesRows = ctx.valuesRow();
                    logger.info("Parsed {} value rows from INSERT statement", valuesRows.size());
                    
                    for (MiniPaimonSQLParser.ValuesRowContext valuesRow : valuesRows) {
                        // 解析单行的值
                        Object[] rowValues = new Object[schema.getFields().size()];
                        List<MiniPaimonSQLParser.ExpressionContext> expressions = valuesRow.expression();
                        
                        for (int i = 0; i < expressions.size(); i++) {
                            String columnName = columnNames.get(i);
                            int fieldIndex = schema.getFieldIndex(columnName);
                            if (fieldIndex >= 0) {
                                Field field = schema.getFields().get(fieldIndex);
                                rowValues[fieldIndex] = parseValue(expressions.get(i), field.getType());
                            }
                        }
                        
                        logger.debug("Writing row: {}", java.util.Arrays.toString(rowValues));
                        writer.write(new Row(rowValues));
                        rowCount++;
                    }
                    
                    logger.info("Wrote {} rows to writer", rowCount);
                    TableWrite.TableCommitMessage commitMessage = writer.prepareCommit();
                    TableCommit commit = table.newCommit();
                    commit.commit(commitMessage, Snapshot.CommitKind.APPEND);
                    System.out.println("Data inserted into table '" + identifier.getFullName() + "' successfully.");
                }
                
            } catch (Exception e) {
                logger.error("Failed to execute INSERT", e);
                throw new RuntimeException("Failed to execute INSERT: " + e.getMessage(), e);
            }
            return null;
        }
        
        @Override
        public Void visitSelectStatement(MiniPaimonSQLParser.SelectStatementContext ctx) {
            try {
                String tableName = ctx.qualifiedName().getText();
                Identifier identifier = parseIdentifier(tableName);
                
                Schema schema = catalog.getTableSchema(identifier);
                if (schema == null) {
                    throw new IOException("Table not found: " + identifier.getFullName());
                }
                
                // 解析投影
                Projection projection = null;
                List<String> selectedFields = new ArrayList<>();
                for (MiniPaimonSQLParser.SelectItemContext item : ctx.selectItem()) {
                    if (item.getText().equals("*")) {
                        projection = Projection.all();
                        break;
                    } else if (item.expression() != null) {
                        selectedFields.add(item.expression().getText());
                    }
                }
                if (projection == null && !selectedFields.isEmpty()) {
                    projection = Projection.of(selectedFields);
                }
                
                // 解析 WHERE 条件
                Predicate predicate = null;
                if (ctx.WHERE() != null) {
                    predicate = parsePredicate(ctx.booleanExpression(), schema);
                }
                
                // 执行查询
                DataTableScan tableScan = new DataTableScan(pathFactory, identifier.getDatabase(), 
                                                           identifier.getTable(), schema);
                if (predicate != null) {
                    tableScan.withPredicate(predicate);
                }
                DataTableScan.Plan plan = tableScan.plan();
                
                DataTableRead tableRead = new DataTableRead(schema, pathFactory, 
                                                           identifier.getDatabase(), identifier.getTable());
                if (projection != null) {
                    tableRead.withProjection(projection);
                }
                if (predicate != null) {
                    tableRead.withFilter(predicate);
                }
                
                List<Row> rows = tableRead.read(plan);
                
                // 打印结果
                Schema resultSchema = projection != null ? projection.projectSchema(schema) : schema;
                printResults(resultSchema, rows);
                
            } catch (Exception e) {
                logger.error("Failed to execute SELECT", e);
                throw new RuntimeException("Failed to execute SELECT: " + e.getMessage(), e);
            }
            return null;
        }
        
        @Override
        public Void visitDropTableStatement(MiniPaimonSQLParser.DropTableStatementContext ctx) {
            try {
                String tableName = ctx.qualifiedName().getText();
                Identifier identifier = parseIdentifier(tableName);
                boolean ifExists = ctx.IF() != null && ctx.EXISTS() != null;
                
                catalog.dropTable(identifier, ifExists);
                System.out.println("Table '" + identifier.getFullName() + "' dropped successfully.");
                
            } catch (CatalogException e) {
                if (ctx.IF() == null || ctx.EXISTS() == null) {
                    logger.error("Failed to execute DROP TABLE", e);
                    throw new RuntimeException("Failed to execute DROP TABLE: " + e.getMessage(), e);
                }
                System.out.println("Table does not exist (ignored).");
            } catch (Exception e) {
                logger.error("Failed to execute DROP TABLE", e);
                throw new RuntimeException("Failed to execute DROP TABLE: " + e.getMessage(), e);
            }
            return null;
        }
        
        @Override
        public Void visitDeleteStatement(MiniPaimonSQLParser.DeleteStatementContext ctx) {
            try {
                String tableName = ctx.qualifiedName().getText();
                Identifier identifier = parseIdentifier(tableName);
                
                Schema schema = catalog.getTableSchema(identifier);
                if (schema == null) {
                    throw new IOException("Table not found: " + identifier.getFullName());
                }
                
                // 获取表
                Table table = catalog.getTable(identifier);
                
                // 如果有 WHERE 条件，使用过滤后重写
                if (ctx.WHERE() != null) {
                    Predicate predicate = parsePredicate(ctx.booleanExpression(), schema);
                    
                    // 1. 读取所有数据
                    TableScan scan = table.newScan().withLatestSnapshot();
                    TableScan.Plan plan = scan.plan();
                    TableRead reader = table.newRead();
                    List<Row> allRows = reader.read(plan);
                    
                    // 2. 过滤掉需要删除的行
                    List<Row> remainingRows = new ArrayList<>();
                    for (Row row : allRows) {
                        if (!predicate.test(row, schema)) {
                            remainingRows.add(row);
                        }
                    }
                    
                    // 3. 重写数据
                    try (TableWrite writer = table.newWrite()) {
                        for (Row row : remainingRows) {
                            writer.write(row);
                        }
                        TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
                        TableCommit commit = table.newCommit();
                        commit.commit(commitMsg, Snapshot.CommitKind.OVERWRITE);
                    }
                    
                    System.out.println("Deleted " + (allRows.size() - remainingRows.size()) + " row(s)");
                } else {
                    // 没有 WHERE 条件，删除所有数据
                    try (TableWrite writer = table.newWrite()) {
                        TableWrite.TableCommitMessage commitMsg = writer.prepareCommit();
                        TableCommit commit = table.newCommit();
                        commit.commit(commitMsg, Snapshot.CommitKind.OVERWRITE);
                    }
                    System.out.println("All data deleted from table");
                }
                
            } catch (Exception e) {
                logger.error("Failed to execute DELETE", e);
                throw new RuntimeException("Failed to execute DELETE: " + e.getMessage(), e);
            }
            return null;
        }
        
        // 辅助方法
        private Identifier parseIdentifier(String qualifiedName) {
            if (qualifiedName.contains(".")) {
                String[] parts = qualifiedName.split("\\.");
                return new Identifier(parts[0], parts[1]);
            }
            return new Identifier(catalog.getDefaultDatabase(), qualifiedName);
        }
        
        private DataType parseDataType(MiniPaimonSQLParser.DataTypeContext ctx) {
            // 获取基础类型名（忽略括号和参数）
            String fullText = ctx.getText();
            String typeName = fullText;
            
            // 如果包含括号，只取括号前面的部分
            int parenIndex = fullText.indexOf('(');
            if (parenIndex > 0) {
                typeName = fullText.substring(0, parenIndex);
            }
            
            typeName = typeName.toUpperCase();
            
            switch (typeName) {
                case "INT":
                case "INTEGER":
                    return DataType.INT();
                case "BIGINT":
                case "LONG":
                    return DataType.LONG();
                case "DOUBLE":
                case "FLOAT":
                    return DataType.DOUBLE();
                case "STRING":
                case "VARCHAR":
                case "CHAR":
                case "TEXT":
                    return DataType.STRING();
                case "BOOLEAN":
                case "BOOL":
                    return DataType.BOOLEAN();
                case "TIMESTAMP":
                case "DATETIME":
                    return DataType.TIMESTAMP();
                default:
                    if (typeName.startsWith("DECIMAL")) {
                        return DataType.DECIMAL(38, 18);
                    } else if (typeName.startsWith("ARRAY")) {
                        return DataType.ARRAY(DataType.STRING());
                    } else if (typeName.startsWith("MAP")) {
                        return DataType.MAP(DataType.STRING(), DataType.STRING());
                    }
                    throw new IllegalArgumentException("Unsupported data type: " + typeName);
            }
        }
        
        private Map<String, List<IndexType>> parseIndexConfig(Map<String, String> properties, List<Field> fields) {
            Map<String, List<IndexType>> indexConfig = new HashMap<>();
            
            // 全局索引配置
            String allIndexTypes = properties.get("file.index.all");
            if (allIndexTypes != null && !allIndexTypes.trim().isEmpty()) {
                List<IndexType> types = parseIndexTypes(allIndexTypes);
                if (!types.isEmpty()) {
                    for (Field field : fields) {
                        indexConfig.put(field.getName(), new ArrayList<>(types));
                    }
                }
            }
            
            // 字段级索引配置
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                
                if (key.startsWith("file.index.") && !key.equals("file.index.all")) {
                    String[] parts = key.split("\\.");
                    if (parts.length == 4 && "true".equalsIgnoreCase(value)) {
                        String fieldName = parts[2];
                        String indexTypeName = parts[3];
                        
                        boolean fieldExists = fields.stream().anyMatch(f -> f.getName().equals(fieldName));
                        if (fieldExists) {
                            try {
                                IndexType indexType = parseIndexType(indexTypeName);
                                indexConfig.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(indexType);
                            } catch (IllegalArgumentException e) {
                                logger.warn("Unknown index type: {}", indexTypeName);
                            }
                        }
                    }
                }
            }
            
            return indexConfig;
        }
        
        private List<IndexType> parseIndexTypes(String indexTypesStr) {
            List<IndexType> types = new ArrayList<>();
            String[] parts = indexTypesStr.split(",");
            for (String part : parts) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    try {
                        types.add(parseIndexType(trimmed));
                    } catch (IllegalArgumentException e) {
                        logger.warn("Unknown index type: {}", trimmed);
                    }
                }
            }
            return types;
        }
        
        private IndexType parseIndexType(String typeName) {
            switch (typeName.toLowerCase().replace("-", "_")) {
                case "bloom_filter":
                    return IndexType.BLOOM_FILTER;
                case "min_max":
                case "minmax":
                    return IndexType.MIN_MAX;
                default:
                    throw new IllegalArgumentException("Unknown index type: " + typeName);
            }
        }
        
        private String formatIndexConfig(Map<String, List<IndexType>> indexConfig) {
            if (indexConfig.isEmpty()) {
                return "none";
            }
            
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<IndexType>> entry : indexConfig.entrySet()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append("[");
                for (int i = 0; i < entry.getValue().size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append(entry.getValue().get(i).getName());
                }
                sb.append("]");
            }
            return sb.toString();
        }
        
        private String unquote(String str) {
            if (str == null || str.length() < 2) {
                return str;
            }
            if ((str.startsWith("'") && str.endsWith("'")) || 
                (str.startsWith("\"") && str.endsWith("\""))) {
                return str.substring(1, str.length() - 1);
            }
            return str;
        }
        
        private Object parseValue(MiniPaimonSQLParser.ExpressionContext ctx, DataType dataType) {
            String text = ctx.getText();
            
            if (text.equalsIgnoreCase("NULL")) {
                return null;
            }
            
            // 移除引号
            if ((text.startsWith("'") && text.endsWith("'")) || 
                (text.startsWith("\"") && text.endsWith("\""))) {
                text = text.substring(1, text.length() - 1);
            }
            
            if (dataType instanceof DataType.IntType) {
                return Integer.parseInt(text);
            } else if (dataType instanceof DataType.LongType) {
                return Long.parseLong(text);
            } else if (dataType instanceof DataType.DoubleType) {
                return Double.parseDouble(text);
            } else if (dataType instanceof DataType.StringType) {
                return text;
            } else if (dataType instanceof DataType.BooleanType) {
                return Boolean.parseBoolean(text);
            } else if (dataType instanceof DataType.TimestampType) {
                return java.time.LocalDateTime.parse(text);
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        }
        
        private Predicate parsePredicate(MiniPaimonSQLParser.BooleanExpressionContext ctx, Schema schema) {
            if (ctx instanceof MiniPaimonSQLParser.ComparisonExpressionContext) {
                MiniPaimonSQLParser.ComparisonExpressionContext comp = 
                    (MiniPaimonSQLParser.ComparisonExpressionContext) ctx;
                
                String fieldName = comp.expression(0).getText();
                String operator = comp.comparisonOperator().getText();
                Object value = parseValue(comp.expression(1), schema.getField(fieldName).getType());
                
                switch (operator) {
                    case "=":
                        return Predicate.equal(fieldName, value);
                    case "<>":
                    case "!=":
                        return Predicate.notEqual(fieldName, value);
                    case ">":
                        return Predicate.greaterThan(fieldName, value);
                    case ">=":
                        return Predicate.greaterOrEqual(fieldName, value);
                    case "<":
                        return Predicate.lessThan(fieldName, value);
                    case "<=":
                        return Predicate.lessOrEqual(fieldName, value);
                    default:
                        throw new IllegalArgumentException("Unsupported operator: " + operator);
                }
            } else if (ctx instanceof MiniPaimonSQLParser.LogicalBinaryExpressionContext) {
                MiniPaimonSQLParser.LogicalBinaryExpressionContext logical = 
                    (MiniPaimonSQLParser.LogicalBinaryExpressionContext) ctx;
                
                Predicate left = parsePredicate(logical.booleanExpression(0), schema);
                Predicate right = parsePredicate(logical.booleanExpression(1), schema);
                
                if (logical.AND() != null) {
                    return left.and(right);
                } else if (logical.OR() != null) {
                    return left.or(right);
                }
            }
            
            throw new UnsupportedOperationException("Unsupported predicate type: " + ctx.getClass());
        }
        
        private void printResults(Schema schema, List<Row> rows) {
            List<Field> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) System.out.print("\t");
                System.out.print(fields.get(i).getName());
            }
            System.out.println();
            
            for (int i = 0; i < fields.size(); i++) {
                if (i > 0) System.out.print("\t");
                System.out.print("--------");
            }
            System.out.println();
            
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
    }
    
    /**
     * 自定义错误监听器
     */
    private static class SQLErrorListener extends org.antlr.v4.runtime.BaseErrorListener {
        @Override
        public void syntaxError(org.antlr.v4.runtime.Recognizer<?, ?> recognizer,
                              Object offendingSymbol,
                              int line, int charPositionInLine,
                              String msg,
                              org.antlr.v4.runtime.RecognitionException e) {
            throw new RuntimeException(
                String.format("Syntax error at line %d:%d - %s", line, charPositionInLine, msg));
        }
    }
}
