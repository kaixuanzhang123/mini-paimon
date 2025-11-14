package com.mini.paimon.read;

import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.reader.KeyValueFileReader;
import com.mini.paimon.reader.KeyValueFileReaderFactory;
import com.mini.paimon.reader.RecordReader;
import com.mini.paimon.table.DataTableScan;
import com.mini.paimon.table.Predicate;
import com.mini.paimon.table.Projection;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并行数据读取器
 * 支持多线程并行读取SST文件,提升大表查询性能
 * 参考 Paimon ParallelReader 实现
 */
public class ParallelDataReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ParallelDataReader.class);
    
    private final Schema schema;
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final KeyValueFileReaderFactory readerFactory;
    
    private Projection projection;
    private Predicate predicate;
    
    // 并行度配置
    private int parallelism;
    private ExecutorService executorService;
    private boolean ownExecutor;
    
    // 批量读取大小
    private int batchSize = 10000;
    
    public ParallelDataReader(Schema schema, PathFactory pathFactory, 
                             String database, String table, int parallelism) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.readerFactory = new KeyValueFileReaderFactory(schema);
        this.parallelism = Math.max(1, parallelism);
        this.executorService = createExecutorService(this.parallelism);
        this.ownExecutor = true;
    }
    
    public ParallelDataReader(Schema schema, PathFactory pathFactory, 
                             String database, String table) {
        this(schema, pathFactory, database, table, 
             Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * 使用外部线程池
     */
    public ParallelDataReader(Schema schema, PathFactory pathFactory,
                             String database, String table,
                             ExecutorService executorService) {
        this.schema = schema;
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.readerFactory = new KeyValueFileReaderFactory(schema);
        this.executorService = executorService;
        this.ownExecutor = false;
        this.parallelism = Runtime.getRuntime().availableProcessors();
    }
    
    public ParallelDataReader withProjection(Projection projection) {
        this.projection = projection;
        return this;
    }
    
    public ParallelDataReader withFilter(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    public ParallelDataReader withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }
    
    /**
     * 并行读取所有数据
     */
    public List<Row> read(DataTableScan.Plan plan) throws IOException {
        if (plan.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<ManifestEntry> dataFiles = plan.getDataFiles();
        logger.info("Starting parallel read of {} files with parallelism {}", 
                   dataFiles.size(), parallelism);
        
        long startTime = System.currentTimeMillis();
        
        try {
            // 分批读取文件
            List<List<ManifestEntry>> batches = splitIntoBatches(dataFiles);
            
            // 提交读取任务
            List<Future<List<Row>>> futures = new ArrayList<>();
            for (List<ManifestEntry> batch : batches) {
                Future<List<Row>> future = executorService.submit(
                    new FileReadTask(batch)
                );
                futures.add(future);
            }
            
            // 收集结果
            List<Row> allRows = new ArrayList<>();
            for (Future<List<Row>> future : futures) {
                try {
                    allRows.addAll(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Read interrupted", e);
                } catch (ExecutionException e) {
                    throw new IOException("Read failed", e.getCause());
                }
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Parallel read completed: {} rows from {} files in {} ms", 
                       allRows.size(), dataFiles.size(), duration);
            
            return allRows;
            
        } catch (Exception e) {
            throw new IOException("Parallel read failed", e);
        }
    }
    
    /**
     * 流式并行读取（减少内存占用）
     */
    public void readStream(DataTableScan.Plan plan, RowConsumer consumer) throws IOException {
        if (plan.isEmpty()) {
            return;
        }
        
        List<ManifestEntry> dataFiles = plan.getDataFiles();
        logger.info("Starting parallel stream read of {} files", dataFiles.size());
        
        long startTime = System.currentTimeMillis();
        AtomicInteger totalRows = new AtomicInteger(0);
        
        try {
            // 使用BlockingQueue进行生产者-消费者模式
            BlockingQueue<Row> rowQueue = new LinkedBlockingQueue<>(batchSize * 2);
            
            // 启动读取线程
            List<Future<?>> futures = new ArrayList<>();
            for (ManifestEntry entry : dataFiles) {
                Future<?> future = executorService.submit(() -> {
                    try {
                        readFileToQueue(entry, rowQueue);
                    } catch (IOException e) {
                        logger.error("Failed to read file: " + entry.getFileName(), e);
                    }
                });
                futures.add(future);
            }
            
            // 启动一个线程来检测所有读取完成
            Future<Boolean> completionFuture = executorService.submit(() -> {
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        logger.error("Read task failed", e);
                    }
                }
                return true;
            });
            
            // 消费数据
            while (!completionFuture.isDone() || !rowQueue.isEmpty()) {
                Row row = rowQueue.poll(100, TimeUnit.MILLISECONDS);
                if (row != null) {
                    consumer.accept(row);
                    totalRows.incrementAndGet();
                }
            }
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Parallel stream read completed: {} rows in {} ms", 
                       totalRows.get(), duration);
            
        } catch (Exception e) {
            throw new IOException("Parallel stream read failed", e);
        }
    }
    
    /**
     * 读取单个文件并放入队列
     */
    private void readFileToQueue(ManifestEntry entry, BlockingQueue<Row> rowQueue) 
            throws IOException {
        DataFileMeta fileMeta = entry.getFile();
        String relativeFilePath = fileMeta.getFileName();
        Path fullFilePath = pathFactory.getTablePath(database, table).resolve(relativeFilePath);
        
        KeyValueFileReaderFactory factory = new KeyValueFileReaderFactory(schema);
        if (predicate != null) {
            factory.withFilter(predicate);
        }
        if (projection != null) {
            factory.withProjection(projection);
        }
        
        try (KeyValueFileReader kvReader = factory.createReader(fullFilePath.toString())) {
            RecordReader<Row> reader = kvReader.readAll();
            Row row;
            while ((row = reader.readRecord()) != null) {
                try {
                    rowQueue.put(row);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while putting row to queue", e);
                }
            }
        }
    }
    
    /**
     * 将文件列表分批
     */
    private List<List<ManifestEntry>> splitIntoBatches(List<ManifestEntry> files) {
        List<List<ManifestEntry>> batches = new ArrayList<>();
        
        // 根据文件大小进行智能分批
        List<FileWithSize> filesWithSize = new ArrayList<>();
        for (ManifestEntry entry : files) {
            filesWithSize.add(new FileWithSize(entry, entry.getFile().getFileSize()));
        }
        
        // 按文件大小降序排序
        filesWithSize.sort((a, b) -> Long.compare(b.size, a.size));
        
        // 使用贪心算法分配到各个batch，尽量均衡总大小
        List<FileBatch> fileBatches = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            fileBatches.add(new FileBatch());
        }
        
        for (FileWithSize file : filesWithSize) {
            // 找到当前总大小最小的batch
            FileBatch minBatch = fileBatches.stream()
                .min(Comparator.comparingLong(b -> b.totalSize))
                .get();
            minBatch.add(file);
        }
        
        // 转换为结果
        for (FileBatch batch : fileBatches) {
            if (!batch.files.isEmpty()) {
                batches.add(batch.files);
            }
        }
        
        logger.debug("Split {} files into {} batches", files.size(), batches.size());
        for (int i = 0; i < batches.size(); i++) {
            logger.debug("Batch {}: {} files", i, batches.get(i).size());
        }
        
        return batches;
    }
    
    /**
     * 创建线程池
     */
    private ExecutorService createExecutorService(int parallelism) {
        return new ThreadPoolExecutor(
            parallelism, parallelism,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "parallel-reader-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }
        );
    }
    
    /**
     * 关闭资源
     */
    public void close() {
        if (ownExecutor && executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
    
    /**
     * 文件读取任务
     */
    private class FileReadTask implements Callable<List<Row>> {
        private final List<ManifestEntry> files;
        
        FileReadTask(List<ManifestEntry> files) {
            this.files = files;
        }
        
        @Override
        public List<Row> call() throws Exception {
            List<Row> rows = new ArrayList<>();
            
            KeyValueFileReaderFactory factory = new KeyValueFileReaderFactory(schema);
            if (predicate != null) {
                factory.withFilter(predicate);
            }
            if (projection != null) {
                factory.withProjection(projection);
            }
            
            for (ManifestEntry entry : files) {
                DataFileMeta fileMeta = entry.getFile();
                String relativeFilePath = fileMeta.getFileName();
                Path fullFilePath = pathFactory.getTablePath(database, table)
                    .resolve(relativeFilePath);
                
                try (KeyValueFileReader kvReader = factory.createReader(fullFilePath.toString())) {
                    RecordReader<Row> reader = kvReader.readAll();
                    Row row;
                    while ((row = reader.readRecord()) != null) {
                        rows.add(row);
                    }
                } catch (IOException e) {
                    logger.error("Failed to read file: " + relativeFilePath, e);
                    throw e;
                }
            }
            
            return rows;
        }
    }
    
    /**
     * 行消费者接口
     */
    public interface RowConsumer {
        void accept(Row row) throws IOException;
    }
    
    /**
     * 带大小信息的文件
     */
    private static class FileWithSize {
        final ManifestEntry entry;
        final long size;
        
        FileWithSize(ManifestEntry entry, long size) {
            this.entry = entry;
            this.size = size;
        }
    }
    
    /**
     * 文件批次
     */
    private static class FileBatch {
        final List<ManifestEntry> files = new ArrayList<>();
        long totalSize = 0;
        
        void add(FileWithSize file) {
            files.add(file.entry);
            totalSize += file.size;
        }
    }
}
