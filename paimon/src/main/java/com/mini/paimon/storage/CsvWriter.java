package com.mini.paimon.storage;

import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * CSV 格式写入器
 * 参考 Paimon CsvFormatWriter 设计
 * 
 * 将数据以 CSV 格式写入文件
 */
public class CsvWriter implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(CsvWriter.class);
    
    private static final String DEFAULT_DELIMITER = ",";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_QUOTE = "\"";
    
    private final Schema schema;
    private final BufferedWriter writer;
    private final String delimiter;
    private final String lineDelimiter;
    private final String quote;
    private final Path filePath;
    
    private long rowCount = 0;
    
    public CsvWriter(Schema schema, Path filePath) throws IOException {
        this(schema, filePath, DEFAULT_DELIMITER, DEFAULT_LINE_DELIMITER, DEFAULT_QUOTE);
    }
    
    public CsvWriter(
            Schema schema, 
            Path filePath,
            String delimiter,
            String lineDelimiter,
            String quote) throws IOException {
        this.schema = schema;
        this.filePath = filePath;
        this.delimiter = delimiter;
        this.lineDelimiter = lineDelimiter;
        this.quote = quote;
        
        // 创建父目录
        Files.createDirectories(filePath.getParent());
        
        // 创建 BufferedWriter
        this.writer = Files.newBufferedWriter(
            filePath, 
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
        
        // 写入表头
        writeHeader();
        
        logger.debug("Created CsvWriter for file: {}", filePath);
    }
    
    /**
     * 写入表头
     */
    private void writeHeader() throws IOException {
        List<Field> fields = schema.getFields();
        StringBuilder header = new StringBuilder();
        
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                header.append(delimiter);
            }
            header.append(escapeValue(fields.get(i).getName()));
        }
        
        writer.write(header.toString());
        writer.write(lineDelimiter);
    }
    
    /**
     * 写入一行数据
     */
    public void write(Row row) throws IOException {
        Object[] values = row.getValues();
        StringBuilder line = new StringBuilder();
        
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                line.append(delimiter);
            }
            
            Object value = values[i];
            if (value != null) {
                line.append(escapeValue(value.toString()));
            }
        }
        
        writer.write(line.toString());
        writer.write(lineDelimiter);
        rowCount++;
    }
    
    /**
     * 转义特殊字符
     */
    private String escapeValue(String value) {
        if (value == null) {
            return "";
        }
        
        // 如果包含分隔符、引号或换行符,需要用引号包围
        if (value.contains(delimiter) || 
            value.contains(quote) || 
            value.contains("\n") || 
            value.contains("\r")) {
            
            // 转义引号
            String escaped = value.replace(quote, quote + quote);
            return quote + escaped + quote;
        }
        
        return value;
    }
    
    /**
     * 刷写缓冲区
     */
    public void flush() throws IOException {
        writer.flush();
    }
    
    /**
     * 获取已写入的行数
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
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        logger.debug("Closed CsvWriter, wrote {} rows to {}", rowCount, filePath);
    }
}

