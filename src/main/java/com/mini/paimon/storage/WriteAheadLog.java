package com.mini.paimon.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mini.paimon.metadata.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-Ahead Log (WAL)
 * 在数据写入内存表前先记录日志，提供崩溃恢复能力
 */
public class WriteAheadLog {
    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);
    
    private final Path walPath;
    private final ObjectMapper objectMapper;
    private FileWriter writer;
    private long sequenceNumber;
    
    public WriteAheadLog(Path walPath) throws IOException {
        this.walPath = walPath;
        this.objectMapper = new ObjectMapper();
        this.sequenceNumber = 0;
        
        // 创建WAL文件
        if (!Files.exists(walPath)) {
            Files.createFile(walPath);
        }
        
        this.writer = new FileWriter(walPath.toFile(), true);
        logger.info("WAL initialized at {}", walPath);
    }
    
    /**
     * 记录写操作
     */
    public synchronized void append(Row row) throws IOException {
        LogEntry entry = new LogEntry(sequenceNumber++, LogEntry.Type.PUT, row);
        String json = objectMapper.writeValueAsString(entry);
        writer.write(json + "\n");
        writer.flush();
    }
    
    /**
     * 恢复数据
     */
    public List<Row> recover() throws IOException {
        List<Row> rows = new ArrayList<>();
        
        if (!Files.exists(walPath) || Files.size(walPath) == 0) {
            return rows;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(walPath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                try {
                    LogEntry entry = objectMapper.readValue(line, LogEntry.class);
                    if (entry.getType() == LogEntry.Type.PUT) {
                        rows.add(entry.getRow());
                    }
                    sequenceNumber = Math.max(sequenceNumber, entry.getSequence() + 1);
                } catch (Exception e) {
                    logger.warn("Failed to parse WAL entry: {}", line, e);
                }
            }
        }
        
        logger.info("Recovered {} rows from WAL", rows.size());
        return rows;
    }
    
    /**
     * 清空WAL
     */
    public synchronized void clear() throws IOException {
        close();
        Files.deleteIfExists(walPath);
        Files.createFile(walPath);
        this.writer = new FileWriter(walPath.toFile(), true);
        this.sequenceNumber = 0;
        logger.info("WAL cleared");
    }
    
    /**
     * 关闭WAL
     */
    public synchronized void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }
    
    /**
     * WAL日志条目
     */
    public static class LogEntry {
        public enum Type {
            PUT, DELETE
        }
        
        private long sequence;
        private Type type;
        private Row row;
        
        public LogEntry() {}
        
        public LogEntry(long sequence, Type type, Row row) {
            this.sequence = sequence;
            this.type = type;
            this.row = row;
        }
        
        public long getSequence() { return sequence; }
        public void setSequence(long sequence) { this.sequence = sequence; }
        
        public Type getType() { return type; }
        public void setType(Type type) { this.type = type; }
        
        public Row getRow() { return row; }
        public void setRow(Row row) { this.row = row; }
    }
}
