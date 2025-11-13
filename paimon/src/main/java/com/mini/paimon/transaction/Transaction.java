package com.mini.paimon.transaction;

import java.time.Instant;
import java.util.UUID;

/**
 * 事务对象
 * 参考 Apache Paimon 的事务设计
 * 
 * 事务生命周期：
 * 1. BEGIN - 开启事务，分配事务ID
 * 2. ACTIVE - 事务活跃，可以执行读写操作
 * 3. PREPARING - 准备提交阶段（两阶段提交的第一阶段）
 * 4. COMMITTED - 事务已提交
 * 5. ABORTED - 事务已回滚
 */
public class Transaction {
    
    /** 事务ID（全局唯一） */
    private final String transactionId;
    
    /** 事务开始时间 */
    private final Instant startTime;
    
    /** 事务的快照ID（读取时的版本） */
    private final long readSnapshotId;
    
    /** 事务状态 */
    private volatile TransactionState state;
    
    /** 事务隔离级别 */
    private final IsolationLevel isolationLevel;
    
    /** 提交时间 */
    private volatile Instant commitTime;
    
    /** 提交的快照ID（写入时生成的版本） */
    private volatile long writeSnapshotId;
    
    /** 事务所属的会话ID */
    private final String sessionId;
    
    /** 是否为只读事务 */
    private final boolean readOnly;
    
    /**
     * 事务状态枚举
     */
    public enum TransactionState {
        /** 事务活跃 */
        ACTIVE,
        /** 准备提交中 */
        PREPARING,
        /** 已提交 */
        COMMITTED,
        /** 已回滚 */
        ABORTED
    }
    
    /**
     * 隔离级别枚举
     * 参考 Paimon 的隔离级别设计
     */
    public enum IsolationLevel {
        /** 读未提交 - 不支持 */
        READ_UNCOMMITTED,
        /** 读已提交 - 每次读取都获取最新已提交的数据 */
        READ_COMMITTED,
        /** 可重复读 - 事务开始时获取快照，整个事务期间读取同一版本 */
        REPEATABLE_READ,
        /** 快照隔离 - 类似可重复读，但有更好的并发控制 */
        SNAPSHOT_ISOLATION,
        /** 可串行化 - 最高隔离级别，通过冲突检测实现 */
        SERIALIZABLE
    }
    
    /**
     * 构造函数
     */
    public Transaction(String transactionId, long readSnapshotId, 
                      IsolationLevel isolationLevel, String sessionId, boolean readOnly) {
        this.transactionId = transactionId;
        this.startTime = Instant.now();
        this.readSnapshotId = readSnapshotId;
        this.state = TransactionState.ACTIVE;
        this.isolationLevel = isolationLevel;
        this.sessionId = sessionId;
        this.readOnly = readOnly;
        this.writeSnapshotId = -1;
    }
    
    /**
     * 创建新事务（默认可读写）
     */
    public static Transaction create(long readSnapshotId, IsolationLevel isolationLevel, String sessionId) {
        String txId = UUID.randomUUID().toString();
        return new Transaction(txId, readSnapshotId, isolationLevel, sessionId, false);
    }
    
    /**
     * 创建只读事务
     */
    public static Transaction createReadOnly(long readSnapshotId, IsolationLevel isolationLevel, String sessionId) {
        String txId = UUID.randomUUID().toString();
        return new Transaction(txId, readSnapshotId, isolationLevel, sessionId, true);
    }
    
    /**
     * 事务进入准备提交状态
     */
    public synchronized void prepare() {
        if (state != TransactionState.ACTIVE) {
            throw new IllegalStateException("Cannot prepare transaction in state: " + state);
        }
        state = TransactionState.PREPARING;
    }
    
    /**
     * 提交事务
     */
    public synchronized void commit(long writeSnapshotId) {
        if (state != TransactionState.PREPARING && state != TransactionState.ACTIVE) {
            throw new IllegalStateException("Cannot commit transaction in state: " + state);
        }
        this.writeSnapshotId = writeSnapshotId;
        this.commitTime = Instant.now();
        this.state = TransactionState.COMMITTED;
    }
    
    /**
     * 回滚事务
     */
    public synchronized void abort() {
        if (state == TransactionState.COMMITTED) {
            throw new IllegalStateException("Cannot abort committed transaction");
        }
        state = TransactionState.ABORTED;
    }
    
    /**
     * 检查事务是否活跃
     */
    public boolean isActive() {
        return state == TransactionState.ACTIVE || state == TransactionState.PREPARING;
    }
    
    /**
     * 检查事务是否已完成（提交或回滚）
     */
    public boolean isFinished() {
        return state == TransactionState.COMMITTED || state == TransactionState.ABORTED;
    }
    
    /**
     * 获取事务持续时间（毫秒）
     */
    public long getDurationMillis() {
        if (commitTime != null) {
            return commitTime.toEpochMilli() - startTime.toEpochMilli();
        }
        return Instant.now().toEpochMilli() - startTime.toEpochMilli();
    }
    
    // Getters
    public String getTransactionId() {
        return transactionId;
    }
    
    public Instant getStartTime() {
        return startTime;
    }
    
    public long getReadSnapshotId() {
        return readSnapshotId;
    }
    
    public TransactionState getState() {
        return state;
    }
    
    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }
    
    public Instant getCommitTime() {
        return commitTime;
    }
    
    public long getWriteSnapshotId() {
        return writeSnapshotId;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public boolean isReadOnly() {
        return readOnly;
    }
    
    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + transactionId + '\'' +
                ", state=" + state +
                ", readSnapshot=" + readSnapshotId +
                ", writeSnapshot=" + writeSnapshotId +
                ", isolation=" + isolationLevel +
                ", readOnly=" + readOnly +
                ", duration=" + getDurationMillis() + "ms" +
                '}';
    }
}
