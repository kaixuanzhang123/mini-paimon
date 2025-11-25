package com.mini.paimon.io;

import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/**
 * Snapshot File
 * 参考 Apache Paimon 的设计，为 Snapshot 提供统一的 I/O 操作
 * - SnapshotFile 负责所有 I/O 操作
 * - 基于 ObjectsFile 框架（但 Snapshot 只处理单个对象，所以更简单）
 * 
 * @see Snapshot 纯数据类
 * @see ObjectsFile 统一的 I/O 框架
 */
public class SnapshotFile extends ObjectsFile<Snapshot> {
    
    public SnapshotFile(PathFactory pathFactory, String database, String table) {
        // Snapshot 不需要 rolling，suggestedFileSize 设为 0
        super(pathFactory, database, table, Snapshot.class, 0L);
    }
    
    /**
     * 写入 snapshot 到指定 ID
     * 
     * @param snapshot snapshot 对象
     * @param snapshotId snapshot ID
     * @throws IOException 写入失败
     */
    public void writeSnapshot(Snapshot snapshot, long snapshotId) throws IOException {
        String fileName = getSnapshotFileName(snapshotId);
        Path filePath = getFilePath(fileName);
        
        Files.createDirectories(filePath.getParent());
        SerializationUtils.writeToFile(filePath, snapshot);
    }
    
    /**
     * 读取指定 ID 的 snapshot
     * 
     * @param snapshotId snapshot ID
     * @return snapshot 对象
     * @throws IOException 读取失败或文件不存在
     */
    public Snapshot readSnapshot(long snapshotId) throws IOException {
        String fileName = getSnapshotFileName(snapshotId);
        Path filePath = getFilePath(fileName);
        
        if (!Files.exists(filePath)) {
            throw new IOException("Snapshot file not found: " + filePath);
        }
        
        return SerializationUtils.readFromFile(filePath, Snapshot.class);
    }
    
    /**
     * 写入 snapshot 到临时文件（用于原子提交）
     * 
     * @param snapshot snapshot 对象
     * @param snapshotId snapshot ID
     * @return 临时文件路径
     * @throws IOException 写入失败
     */
    public Path writeTempSnapshot(Snapshot snapshot, long snapshotId) throws IOException {
        String fileName = getSnapshotFileName(snapshotId) + ".tmp";
        Path tempPath = getFilePath(fileName);
        
        Files.createDirectories(tempPath.getParent());
        SerializationUtils.writeToFile(tempPath, snapshot);
        
        return tempPath;
    }
    
    /**
     * 检查指定 snapshot 是否存在
     * 
     * @param snapshotId snapshot ID
     * @return true 如果存在
     */
    public boolean snapshotExists(long snapshotId) {
        String fileName = getSnapshotFileName(snapshotId);
        Path filePath = getFilePath(fileName);
        return Files.exists(filePath);
    }
    
    /**
     * 获取 snapshot 文件路径
     * 
     * @param snapshotId snapshot ID
     * @return 文件路径
     */
    public Path getSnapshotPath(long snapshotId) {
        String fileName = getSnapshotFileName(snapshotId);
        return getFilePath(fileName);
    }
    
    @Override
    protected Path getFilePath(String fileName) {
        // snapshot 文件存储在 snapshot 目录下
        return pathFactory.getSnapshotDir(database, table).resolve(fileName);
    }
    
    @Override
    protected String generateFileName() {
        // Snapshot 的文件名由 snapshotId 决定，不需要自动生成
        throw new UnsupportedOperationException(
            "SnapshotFile does not support auto-generated file names. " +
            "Use writeSnapshot(snapshot, snapshotId) instead.");
    }
    
    /**
     * 生成 snapshot 文件名
     * 
     * @param snapshotId snapshot ID
     * @return 文件名（snapshot-{id}）
     */
    private String getSnapshotFileName(long snapshotId) {
        return "snapshot-" + snapshotId;
    }
    
    // Note: write() 和 read() 方法继承自 ObjectsFile，但不推荐直接使用
    // 应该使用 writeSnapshot() 和 readSnapshot() 以获得更好的语义
}

