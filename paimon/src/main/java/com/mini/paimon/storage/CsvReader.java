package com.mini.paimon.storage;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV 格式读取器
 * 参考 Paimon CsvFormatReader 设计
 * 
 * 从 CSV 文件读取数据并转换为 Row 对象
 */
public class CsvReader implements AutoCloseable {
    
    private static final String DEFAULT_DELIMITER = ",";
    private static final String DEFAULT_QUOTE = "\"";
    
    private final Schema schema;
    private final BufferedReader reader;
    private final String delimiter;
    private final String quote;
    private final Path filePath;
    
    private boolean headerSkipped = false;
    private long rowCount = 0;
    
    public CsvReader(Schema schema, Path filePath) throws IOException {
        this(schema, filePath, DEFAULT_DELIMITER, DEFAULT_QUOTE);
    }
    
    public CsvReader(
            Schema schema, 
            Path filePath,
            String delimiter,
            String quote) throws IOException {
        this.schema = schema;
        this.filePath = filePath;
        this.delimiter = delimiter;
        this.quote = quote;
        
        // 创建 BufferedReader
        this.reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8);
    }
    
    /**
     * 读取下一行数据
     * @return Row 对象，如果到达文件末尾返回 null
     */
    public Row readRow() throws IOException {
        // 跳过表头
        if (!headerSkipped) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return null;
            }
            headerSkipped = true;
        }
        
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        
        // 解析 CSV 行
        List<String> values = parseCsvLine(line);
        
        // 转换为 Row 对象
        Row row = convertToRow(values);
        rowCount++;
        
        return row;
    }
    
    /**
     * 读取所有行
     */
    public List<Row> readAll() throws IOException {
        List<Row> rows = new ArrayList<>();
        Row row;
        while ((row = readRow()) != null) {
            rows.add(row);
        }
        return rows;
    }
    
    /**
     * 解析 CSV 行
     * 处理引号和转义字符
     */
    private List<String> parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder currentValue = new StringBuilder();
        boolean inQuotes = false;
        boolean lastCharWasQuote = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == quote.charAt(0)) {
                if (inQuotes) {
                    if (lastCharWasQuote) {
                        // 双引号转义
                        currentValue.append(c);
                        lastCharWasQuote = false;
                    } else {
                        lastCharWasQuote = true;
                    }
                } else {
                    inQuotes = true;
                }
            } else if (c == delimiter.charAt(0) && !inQuotes) {
                // 字段分隔符
                values.add(currentValue.toString());
                currentValue.setLength(0);
                lastCharWasQuote = false;
            } else {
                if (lastCharWasQuote) {
                    inQuotes = false;
                    lastCharWasQuote = false;
                }
                currentValue.append(c);
            }
        }
        
        // 添加最后一个字段
        values.add(currentValue.toString());
        
        return values;
    }
    
    /**
     * 将字符串值转换为 Row 对象
     */
    private Row convertToRow(List<String> values) throws IOException {
        List<Field> fields = schema.getFields();
        
        if (values.size() != fields.size()) {
            throw new IOException(String.format(
                "CSV column count mismatch: expected %d, got %d", 
                fields.size(), values.size()));
        }
        
        Object[] rowValues = new Object[fields.size()];
        
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            String strValue = values.get(i);
            
            // 处理空值
            if (strValue == null || strValue.isEmpty()) {
                rowValues[i] = null;
                continue;
            }
            
            // 根据字段类型转换
            rowValues[i] = convertValue(strValue, field.getType());
        }
        
        return new Row(rowValues);
    }
    
    /**
     * 将字符串值转换为指定类型
     */
    private Object convertValue(String strValue, DataType dataType) throws IOException {
        try {
            if (dataType instanceof DataType.IntType) {
                return Integer.parseInt(strValue);
            } else if (dataType instanceof DataType.LongType) {
                return Long.parseLong(strValue);
            } else if (dataType instanceof DataType.DoubleType) {
                return Double.parseDouble(strValue);
            } else if (dataType instanceof DataType.BooleanType) {
                return Boolean.parseBoolean(strValue);
            } else if (dataType instanceof DataType.StringType) {
                return strValue;
            } else {
                throw new IOException("Unsupported data type: " + dataType);
            }
        } catch (NumberFormatException e) {
            throw new IOException(
                String.format("Failed to convert value '%s' to type %s", strValue, dataType), e);
        }
    }
    
    /**
     * 获取已读取的行数
     */
    public long getRowCount() {
        return rowCount;
    }
    
    /**
     * 获取文件路径
     */
    public Path getFilePath() {
        return filePath;
    }
    
    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}

