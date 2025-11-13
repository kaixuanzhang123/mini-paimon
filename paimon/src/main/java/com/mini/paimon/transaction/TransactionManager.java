package com.mini.paimon.transaction;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.metadata.RowKey;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.table.Table;
import com.mini.paimon.table.TableCommit;
import com.mini.paimon.table.TableWrite;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * 事务管理器
 * 参考 Apache Paimon 的 MVCC 设计
 * 
 * 核心功能：
 * 1. 事务生命周期管理
 * 2. MVCC 多版本并发控制
 * 3. 事务冲突检测（乐观锁和悲观锁）
 * 4. 隔离级别控制
 * 5. 死锁检测和预防
 * 
 * MVCC 实现原理：
 * - 每个事务读取特定版本的快照
 * - 写操作不会覆盖原数据，而是创建新版本
 * - 通过快照隔离避免读写冲突
 * - 写写冲突通过冲突检测解决
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);
    
    /** 活跃事务映射 */
    private final ConcurrentHashMap<String, Transaction> activeTransactions;
    
    /** 已完成事务历史（用于冲突检测） */
    private final CopyOnWriteArrayList<Transaction> completedTransactions;
    
    /** 事务写集（记录每个事务的写操作） */
    private final ConcurrentHashMap<String, Set<RowKey>> transactionWriteSets;
    
    /** 事务锁管理（悲观锁模式） */
    private final ConcurrentHashMap<RowKey, String> rowLocks;
    
    /** 全局读写锁（保护关键操作） */
    private final ReadWriteLock globalLock;
    
    /** 目录管理器 */
    private final Catalog catalog;
    
    /** 路径工厂 */
    private final PathFactory pathFactory;
    
    /** 默认隔离级别 */
    private final Transaction.IsolationLevel defaultIsolationLevel;
    
    /** 事务超时时间（毫秒） */
    private final long transactionTimeoutMillis;
    
    /** 冲突重试次数 */
    private final int conflictRetryCount;
    
    /** 最大活跃事务数 */
    private final int maxActiveTransactions;
    
    /** 统计信息 */
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final AtomicLong committedTransactions = new AtomicLong(0);
    private final AtomicLong abortedTransactions = new AtomicLong(0);
    private final AtomicLong conflictCount = new AtomicLong(0);
    
    /**
     * 构造函数
     */
    public TransactionManager(Catalog catalog, PathFactory pathFactory) {
        this(catalog, pathFactory, Transaction.IsolationLevel.SNAPSHOT_ISOLATION);
    }
    
    /**
     * 构造函数（指定默认隔离级别）
     */
    public TransactionManager(Catalog catalog, PathFactory pathFactory, 
                            Transaction.IsolationLevel defaultIsolationLevel) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.defaultIsolationLevel = defaultIsolationLevel;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.completedTransactions = new CopyOnWriteArrayList<>();
        this.transactionWriteSets = new ConcurrentHashMap<>();
        this.rowLocks = new ConcurrentHashMap<>();
        this.globalLock = new ReentrantReadWriteLock();
        this.transactionTimeoutMillis = 300000; // 5分钟
        this.conflictRetryCount = 3;
        this.maxActiveTransactions = 100;
        
        // 启动后台清理线程
        startCleanupThread();
    }
    
    /**
     * 开始新事务
     */
    public Transaction beginTransaction(Identifier tableId, String sessionId) throws IOException {
        return beginTransaction(tableId, sessionId, defaultIsolationLevel, false);
    }
    
    /**
     * 开始只读事务
     */
    public Transaction beginReadOnlyTransaction(Identifier tableId, String sessionId) throws IOException {
        return beginTransaction(tableId, sessionId, defaultIsolationLevel, true);
    }
    
    /**
     * 开始事务（指定隔离级别）
     */
    public Transaction beginTransaction(Identifier tableId, String sessionId, 
                                       Transaction.IsolationLevel isolationLevel,
                                       boolean readOnly) throws IOException {
        // 检查活跃事务数限制
        if (activeTransactions.size() >= maxActiveTransactions) {
            throw new IllegalStateException("Too many active transactions: " + activeTransactions.size());
        }
        
        globalLock.readLock().lock();
        try {
            // 获取当前最新的快照ID作为读取版本
            SnapshotManager snapshotManager = new SnapshotManager(
                pathFactory, tableId.getDatabase(), tableId.getTable());
            
            long readSnapshotId;
            if (snapshotManager.hasSnapshot()) {
                Snapshot latestSnapshot = snapshotManager.getLatestSnapshot();
                readSnapshotId = latestSnapshot.getId();
            } else {
                readSnapshotId = 0; // 空表
            }
            
            // 创建事务
            Transaction transaction = readOnly ? 
                Transaction.createReadOnly(readSnapshotId, isolationLevel, sessionId) :
                Transaction.create(readSnapshotId, isolationLevel, sessionId);
            
            // 注册事务
            activeTransactions.put(transaction.getTransactionId(), transaction);
            if (!readOnly) {
                transactionWriteSets.put(transaction.getTransactionId(), new HashSet<>());
            }
            
            totalTransactions.incrementAndGet();
            
            logger.info("Started {} transaction {} with read snapshot {} and isolation level {}",
                       readOnly ? "read-only" : "read-write",
                       transaction.getTransactionId(), readSnapshotId, isolationLevel);
            
            return transaction;
        } finally {
            globalLock.readLock().unlock();
        }
    }
    
    /**
     * 记录写操作（用于冲突检测）
     */
    public void recordWrite(String transactionId, RowKey rowKey) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction not found: " + transactionId);
        }
        
        if (transaction.isReadOnly()) {
            throw new IllegalStateException("Cannot write in read-only transaction");
        }
        
        Set<RowKey> writeSet = transactionWriteSets.get(transactionId);
        if (writeSet != null) {
            writeSet.add(rowKey);
        }
    }
    
    /**
     * 获取行锁（悲观锁模式）
     */
    public boolean acquireRowLock(String transactionId, RowKey rowKey, long timeoutMillis) {
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            String existingLock = rowLocks.putIfAbsent(rowKey, transactionId);
            if (existingLock == null || existingLock.equals(transactionId)) {
                return true; // 成功获取锁
            }
            
            // 检查持有锁的事务是否还活跃
            if (!activeTransactions.containsKey(existingLock)) {
                // 锁持有者已经不活跃，尝试释放并重新获取
                rowLocks.remove(rowKey, existingLock);
                continue;
            }
            
            // 等待一段时间后重试
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return false; // 超时
    }
    
    /**
     * 释放行锁
     */
    public void releaseRowLock(String transactionId, RowKey rowKey) {
        rowLocks.remove(rowKey, transactionId);
    }
    
    /**
     * 释放事务的所有锁
     */
    private void releaseAllLocks(String transactionId) {
        rowLocks.entrySet().removeIf(entry -> entry.getValue().equals(transactionId));
    }
    
    /**
     * 准备提交事务（两阶段提交的第一阶段）
     */
    public boolean prepareCommit(String transactionId) throws IOException {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction not found: " + transactionId);
        }
        
        if (transaction.isReadOnly()) {
            // 只读事务直接提交
            transaction.prepare();
            return true;
        }
        
        globalLock.readLock().lock();
        try {
            transaction.prepare();
            
            // 执行冲突检测
            if (!checkConflicts(transaction)) {
                logger.warn("Transaction {} conflicts detected, aborting", transactionId);
                conflictCount.incrementAndGet();
                return false;
            }
            
            logger.debug("Transaction {} prepared successfully", transactionId);
            return true;
        } finally {
            globalLock.readLock().unlock();
        }
    }
    
    /**
     * 提交事务（两阶段提交的第二阶段）
     */
    public void commitTransaction(String transactionId, long writeSnapshotId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("Transaction not found: " + transactionId);
        }
        
        globalLock.writeLock().lock();
        try {
            transaction.commit(writeSnapshotId);
            
            // 移动到已完成列表
            activeTransactions.remove(transactionId);
            completedTransactions.add(transaction);
            
            // 清理写集
            transactionWriteSets.remove(transactionId);
            
            // 释放所有锁
            releaseAllLocks(transactionId);
            
            committedTransactions.incrementAndGet();
            
            logger.info("Transaction {} committed with write snapshot {}", 
                       transactionId, writeSnapshotId);
            
            // 清理过期的已完成事务
            cleanupCompletedTransactions();
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    /**
     * 回滚事务
     */
    public void abortTransaction(String transactionId) {
        Transaction transaction = activeTransactions.get(transactionId);
        if (transaction == null) {
            return; // 事务已经不存在
        }
        
        globalLock.writeLock().lock();
        try {
            transaction.abort();
            
            // 清理
            activeTransactions.remove(transactionId);
            transactionWriteSets.remove(transactionId);
            releaseAllLocks(transactionId);
            
            abortedTransactions.incrementAndGet();
            
            logger.info("Transaction {} aborted", transactionId);
        } finally {
            globalLock.writeLock().unlock();
        }
    }
    
    /**
     * 冲突检测
     * 
     * 检测规则（基于隔离级别）：
     * 1. SNAPSHOT_ISOLATION: 检查写写冲突
     * 2. SERIALIZABLE: 检查读写和写写冲突
     */
    private boolean checkConflicts(Transaction transaction) {
        if (transaction.isReadOnly()) {
            return true; // 只读事务不会冲突
        }
        
        Set<RowKey> writeSet = transactionWriteSets.get(transaction.getTransactionId());
        if (writeSet == null || writeSet.isEmpty()) {
            return true; // 没有写操作
        }
        
        long readSnapshotId = transaction.getReadSnapshotId();
        
        // 检查与已提交事务的冲突
        for (Transaction committed : completedTransactions) {
            if (committed.getState() != Transaction.TransactionState.COMMITTED) {
                continue;
            }
            
            // 只检查在当前事务开始后提交的事务
            if (committed.getWriteSnapshotId() <= readSnapshotId) {
                continue;
            }
            
            Set<RowKey> committedWriteSet = transactionWriteSets.get(committed.getTransactionId());
            if (committedWriteSet != null) {
                // 检查写集是否有交集
                for (RowKey key : writeSet) {
                    if (committedWriteSet.contains(key)) {
                        logger.warn("Write-write conflict detected: transaction {} conflicts with committed transaction {} on key {}",
                                   transaction.getTransactionId(), committed.getTransactionId(), key);
                        return false;
                    }
                }
            }
        }
        
        // 检查与其他活跃事务的冲突（仅在 SERIALIZABLE 级别）
        if (transaction.getIsolationLevel() == Transaction.IsolationLevel.SERIALIZABLE) {
            for (Transaction active : activeTransactions.values()) {
                if (active.getTransactionId().equals(transaction.getTransactionId())) {
                    continue;
                }
                
                if (active.getState() == Transaction.TransactionState.PREPARING) {
                    // 另一个事务正在准备提交，可能冲突
                    Set<RowKey> activeWriteSet = transactionWriteSets.get(active.getTransactionId());
                    if (activeWriteSet != null) {
                        for (RowKey key : writeSet) {
                            if (activeWriteSet.contains(key)) {
                                logger.warn("Potential write-write conflict with active transaction {} on key {}",
                                           active.getTransactionId(), key);
                                return false;
                            }
                        }
                    }
                }
            }
        }
        
        return true;
    }
    
    /**
     * 清理过期的已完成事务
     */
    private void cleanupCompletedTransactions() {
        long cutoffTime = System.currentTimeMillis() - 3600000; // 保留1小时
        completedTransactions.removeIf(tx -> {
            if (tx.getCommitTime() != null) {
                return tx.getCommitTime().toEpochMilli() < cutoffTime;
            }
            return tx.getStartTime().toEpochMilli() < cutoffTime;
        });
    }
    
    /**
     * 清理超时的活跃事务
     */
    private void cleanupTimeoutTransactions() {
        long now = System.currentTimeMillis();
        List<String> timeoutTransactions = new ArrayList<>();
        
        for (Transaction transaction : activeTransactions.values()) {
            if (now - transaction.getStartTime().toEpochMilli() > transactionTimeoutMillis) {
                timeoutTransactions.add(transaction.getTransactionId());
            }
        }
        
        for (String txId : timeoutTransactions) {
            logger.warn("Transaction {} timeout, aborting", txId);
            abortTransaction(txId);
        }
    }
    
    /**
     * 启动后台清理线程
     */
    private void startCleanupThread() {
        Thread cleanupThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(60000); // 每分钟清理一次
                    cleanupTimeoutTransactions();
                    cleanupCompletedTransactions();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in cleanup thread", e);
                }
            }
        }, "TransactionManager-Cleanup");
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }
    
    /**
     * 获取事务统计信息
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalTransactions", totalTransactions.get());
        stats.put("committedTransactions", committedTransactions.get());
        stats.put("abortedTransactions", abortedTransactions.get());
        stats.put("activeTransactions", activeTransactions.size());
        stats.put("completedTransactions", completedTransactions.size());
        stats.put("conflictCount", conflictCount.get());
        stats.put("rowLocks", rowLocks.size());
        
        // 计算成功率
        long total = committedTransactions.get() + abortedTransactions.get();
        if (total > 0) {
            double successRate = (double) committedTransactions.get() / total * 100;
            stats.put("successRate", String.format("%.2f%%", successRate));
        }
        
        return stats;
    }
    
    /**
     * 获取活跃事务列表
     */
    public List<Transaction> getActiveTransactions() {
        return new ArrayList<>(activeTransactions.values());
    }
    
    /**
     * 获取特定事务
     */
    public Transaction getTransaction(String transactionId) {
        return activeTransactions.get(transactionId);
    }
    
    /**
     * 检查事务是否存在
     */
    public boolean hasTransaction(String transactionId) {
        return activeTransactions.containsKey(transactionId);
    }
    
    @Override
    public String toString() {
        return "TransactionManager{" +
                "activeTransactions=" + activeTransactions.size() +
                ", defaultIsolation=" + defaultIsolationLevel +
                ", statistics=" + getStatistics() +
                '}';
    }
}
