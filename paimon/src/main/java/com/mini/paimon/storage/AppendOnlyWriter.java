package com.mini.paimon.storage;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AppendOnlyWriter - 仅追加表写入器
 * 参考 Paimon AppendOnlyWriter 和 CsvFormatWriter 设计
 * 
 * 特点:
 * 1. 不使用 LSM Tree
 * 2. 直接写 CSV 文件
 * 3. 只支持 INSERT
 * 4. 不需要 Compaction
 * 5. 性能更高,因为没有合并开销
 */
public class AppendOnlyWriter implements RecordWriter {
    
    private static final Logger logger = LoggerFactory.getLogger(AppendOnlyWriter.class);
    
    private static final int DEFAULT_BUFFER_SIZE = 1000;
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String tableName;
    private final long writerId;
    private final AtomicLong sequenceGenerator;
    
    // 当前的 CSV Writer
    private CsvWriter currentWriter;
    
    // 当前缓冲的行数
    private int currentBufferSize = 0;
    
    // 已刷写的文件
    private final List<DataFileMeta> flushedFiles;
    
    // 当前文件的起始行
    private Row firstRow = null;
    private Row lastRow = null;
    
    public AppendOnlyWriter(
            Schema schema,
            PathFactory pathFactory,
            String database,
            String tableName,
            long writerId) throws IOException {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.tableName = tableName;
        this.writerId = writerId;
        
        // 初始化序列号生成器
        long initialSequence = (writerId << 32) | (System.currentTimeMillis() & 0xFFFFFFFFL);
        this.sequenceGenerator = new AtomicLong(initialSequence);
        
        // 初始化文件列表
        this.flushedFiles = new ArrayList<>();
        
        // 创建第一个 CSV Writer
        createNewWriter();
        
        logger.info("Created AppendOnlyWriter for table {}.{} with writerId={}", 
            database, tableName, writerId);
    }
    
    /**
     * 创建新的 CSV Writer
     */
    private void createNewWriter() throws IOException {
        // 关闭旧的 writer
        if (currentWriter != null) {
            closeCurrentWriter();
        }
        
        // 生成文件路径 (使用 .csv 扩展名)
        long sequence = sequenceGenerator.getAndIncrement();
        Path csvPath = getCsvPath(sequence);
        
        // 创建新的 CSV Writer
        currentWriter = new CsvWriter(schema, csvPath);
        currentBufferSize = 0;
        firstRow = null;
        lastRow = null;
        
        logger.debug("Created new CSV writer: {}", csvPath);
    }
    
    /**
     * 获取 CSV 文件路径
     */
    private Path getCsvPath(long sequence) {
        Path dataDir = pathFactory.getDataDir(database, tableName);
        String fileName = String.format("data-%d-%d.csv", 0, sequence);
        return dataDir.resolve(fileName);
    }
    
    /**
     * 关闭当前 Writer 并记录文件元信息
     */
    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }
        
        currentWriter.flush();
        long rowCount = currentWriter.getRowCount();
        Path filePath = currentWriter.getFilePath();
        currentWriter.close();
        
        // 获取文件大小
        long fileSize = Files.size(filePath);
        
        // 创建文件元信息
        // 注意: CSV 文件没有 minKey/maxKey 的概念,使用 null
        // CsvWriter.getRowCount() 已经只统计数据行（不包括表头），所以这里不需要再减1
        String fileName = filePath.getFileName().toString();
        DataFileMeta fileMeta = new DataFileMeta(
            fileName,
            fileSize,
            rowCount,  // CsvWriter已经返回实际数据行数
            null,  // CSV 文件没有 minKey
            null,  // CSV 文件没有 maxKey
            schema.getSchemaId(),
            0,  // level 0
            System.currentTimeMillis()
        );
        
        flushedFiles.add(fileMeta);
        
        logger.info("Closed CSV file: {}, rows: {}, size: {} bytes", 
            fileName, rowCount, fileSize);
    }
    
    @Override
    public void write(Row row) throws IOException {
        // 记录第一行和最后一行 (用于统计)
        if (firstRow == null) {
            firstRow = row;
        }
        lastRow = row;
        
        // 写入当前 CSV 文件
        currentWriter.write(row);
        currentBufferSize++;
        
        // 检查是否需要创建新文件
        if (currentBufferSize >= DEFAULT_BUFFER_SIZE) {
            createNewWriter();
        }
    }
    
    @Override
    public List<DataFileMeta> prepareCommit() throws IOException {
        logger.info("AppendOnlyWriter preparing commit for table {}.{}", database, tableName);
        
        // 关闭当前 writer
        if (currentWriter != null) {
            closeCurrentWriter();
            currentWriter = null;
        }
        
        // 返回所有文件
        List<DataFileMeta> files = new ArrayList<>(flushedFiles);
        
        logger.info("AppendOnlyWriter prepared {} CSV files for table {}.{}", 
            files.size(), database, tableName);
        
        return files;
    }
    
    @Override
    public void close() throws IOException {
        // 关闭当前 writer
        if (currentWriter != null) {
            closeCurrentWriter();
            currentWriter = null;
        }
        
        logger.info("Closed AppendOnlyWriter for table {}.{}", database, tableName);
    }
}
