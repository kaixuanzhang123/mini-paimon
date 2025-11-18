package com.mini.paimon.storage;

import com.mini.paimon.schema.Schema;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 异步 Compaction 执行器
 * 参考 Paimon 设计，使用后台线程池异步执行 Compaction
 * 
 * 核心功能：
 * 1. 异步执行 Compaction，不阻塞写入
 * 2. 支持任务调度和优先级管理
 * 3. 提供任务状态查询和监控
 */
public class AsyncCompactor {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCompactor.class);
    
    private final Compactor compactor;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;
    
    // 统计信息
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong totalDuration = new AtomicLong(0);
    
    // Compaction 回调接口
    public interface CompactionCallback {
        void onSuccess(Compactor.CompactionResult result);
        void onFailure(Exception e);
    }
    
    public AsyncCompactor(Schema schema, PathFactory pathFactory, 
                         String database, String table,
                         AtomicLong sequenceGenerator) {
        this(schema, pathFactory, database, table, sequenceGenerator, null);
    }
    
    public AsyncCompactor(Schema schema, PathFactory pathFactory, 
                         String database, String table,
                         AtomicLong sequenceGenerator,
                         java.util.Map<String, List<com.mini.paimon.index.IndexType>> indexConfig) {
        this.compactor = new Compactor(schema, pathFactory, database, table, sequenceGenerator, indexConfig);
        
        // 创建专用线程池（核心线程数=1，避免并发 Compaction 冲突）
        this.executor = new ThreadPoolExecutor(
            1,  // 核心线程数
            1,  // 最大线程数
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(100),  // 任务队列
            new ThreadFactory() {
                private final AtomicLong counter = new AtomicLong(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "async-compactor-" + counter.incrementAndGet());
                    t.setDaemon(true);  // 设置为守护线程
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()  // 队列满时由调用线程执行
        );
        
        // 创建定时调度器（用于周期性检查 Compaction 需求）
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "compaction-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        logger.info("AsyncCompactor initialized");
    }
    
    /**
     * 异步提交 Compaction 任务
     * 
     * @param sstables 当前的 SSTable 列表
     * @param callback 完成回调
     * @return Future 对象，用于查询任务状态
     */
    public Future<Compactor.CompactionResult> submitCompaction(
            List<Compactor.LeveledSSTable> sstables,
            CompactionCallback callback) {
        
        return executor.submit(() -> {
            long startTime = System.currentTimeMillis();
            try {
                logger.info("Starting async compaction task");
                
                // 执行 Compaction
                Compactor.CompactionResult result = compactor.compact(sstables);
                
                long duration = System.currentTimeMillis() - startTime;
                totalDuration.addAndGet(duration);
                successCount.incrementAndGet();
                
                logger.info("Async compaction completed successfully in {}ms", duration);
                
                // 回调成功
                if (callback != null) {
                    callback.onSuccess(result);
                }
                
                return result;
                
            } catch (Exception e) {
                long duration = System.currentTimeMillis() - startTime;
                failureCount.incrementAndGet();
                
                logger.error("Async compaction failed after {}ms", duration, e);
                
                // 回调失败
                if (callback != null) {
                    callback.onFailure(e);
                }
                
                throw e;
            }
        });
    }
    
    /**
     * 异步提交 Compaction 任务（无回调）
     */
    public Future<Compactor.CompactionResult> submitCompaction(
            List<Compactor.LeveledSSTable> sstables) {
        return submitCompaction(sstables, null);
    }
    
    /**
     * 启动周期性 Compaction 检查
     * 
     * @param sstablesSupplier SSTable 列表提供者
     * @param callback 完成回调
     * @param period 检查周期（秒）
     */
    public void startPeriodicCompaction(
            Callable<List<Compactor.LeveledSSTable>> sstablesSupplier,
            CompactionCallback callback,
            long period) {
        
        scheduler.scheduleAtFixedRate(() -> {
            try {
                List<Compactor.LeveledSSTable> sstables = sstablesSupplier.call();
                
                // 检查是否需要 Compaction
                if (compactor.needsCompaction(sstables)) {
                    logger.info("Periodic compaction check: compaction needed, submitting task");
                    submitCompaction(sstables, callback);
                } else {
                    logger.debug("Periodic compaction check: no compaction needed");
                }
                
            } catch (Exception e) {
                logger.warn("Periodic compaction check failed", e);
            }
        }, period, period, TimeUnit.SECONDS);
        
        logger.info("Started periodic compaction check with period {}s", period);
    }
    
    /**
     * 检查是否需要 Compaction
     */
    public boolean needsCompaction(List<Compactor.LeveledSSTable> sstables) {
        return compactor.needsCompaction(sstables);
    }
    
    /**
     * 获取统计信息
     */
    public CompactionStats getStats() {
        return new CompactionStats(
            successCount.get(),
            failureCount.get(),
            totalDuration.get()
        );
    }
    
    /**
     * 关闭异步执行器
     */
    public void shutdown() {
        logger.info("Shutting down AsyncCompactor");
        
        scheduler.shutdown();
        executor.shutdown();
        
        try {
            // 等待任务完成（最多 30 秒）
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Executor did not terminate in time, forcing shutdown");
                executor.shutdownNow();
            }
            
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for shutdown", e);
            executor.shutdownNow();
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("AsyncCompactor shut down successfully");
    }
    
    /**
     * Compaction 统计信息
     */
    public static class CompactionStats {
        private final long successCount;
        private final long failureCount;
        private final long totalDuration;
        
        public CompactionStats(long successCount, long failureCount, long totalDuration) {
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.totalDuration = totalDuration;
        }
        
        public long getSuccessCount() {
            return successCount;
        }
        
        public long getFailureCount() {
            return failureCount;
        }
        
        public long getTotalDuration() {
            return totalDuration;
        }
        
        public long getAverageDuration() {
            long total = successCount + failureCount;
            return total == 0 ? 0 : totalDuration / total;
        }
        
        @Override
        public String toString() {
            return String.format("CompactionStats{success=%d, failure=%d, avgDuration=%dms}",
                successCount, failureCount, getAverageDuration());
        }
    }
}
