package com.mini.paimon.table;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Table 提交器
 * 参考 Paimon TableCommit 设计，负责提交快照
 */
public class TableCommit {
    private static final Logger logger = LoggerFactory.getLogger(TableCommit.class);
    
    private final Catalog catalog;
    private final PathFactory pathFactory;
    private final Identifier identifier;
    private final SnapshotManager snapshotManager;
    private final IdGenerator idGenerator;
    
    public TableCommit(Catalog catalog, PathFactory pathFactory, Identifier identifier) {
        this.catalog = catalog;
        this.pathFactory = pathFactory;
        this.identifier = identifier;
        this.snapshotManager = new SnapshotManager(
            pathFactory, identifier.getDatabase(), identifier.getTable());
        this.idGenerator = new IdGenerator();
    }
    
    /**
     * 提交写入操作
     */
    public void commit(TableWrite.TableCommitMessage commitMessage) throws IOException {
        commit(commitMessage, Snapshot.CommitKind.APPEND);
    }
    
    /**
     * 提交写入操作（指定提交类型）
     */
    public void commit(TableWrite.TableCommitMessage commitMessage, Snapshot.CommitKind commitKind) 
            throws IOException {
        
        logger.info("Committing changes to table {}", identifier);
        
        List<ManifestEntry> manifestEntries = collectManifestEntries();
        
        // 对于 OVERWRITE 提交，即使没有数据文件也要创建快照（表示数据被清空）
        if (manifestEntries.isEmpty() && commitKind != Snapshot.CommitKind.OVERWRITE) {
            logger.warn("No data files to commit");
            return;
        }
        
        Snapshot snapshot = snapshotManager.createSnapshot(
            commitMessage.getSchemaId(), 
            manifestEntries, 
            commitKind
        );
        
        try {
            catalog.commitSnapshot(identifier, snapshot);
            logger.info("Successfully committed snapshot {} to table {}", 
                       snapshot.getId(), identifier);
        } catch (CatalogException e) {
            throw new IOException("Failed to commit snapshot", e);
        }
    }
    
    /**
     * 收集 Manifest 条目
     * 支持分区表：扫描表目录下的所有分区子目录（与 snapshot、manifest 同级）
     */
    private List<ManifestEntry> collectManifestEntries() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        
        Path tableDir = pathFactory.getTablePath(identifier.getDatabase(), identifier.getTable());
        if (!Files.exists(tableDir)) {
            return entries;
        }
        
        // 递归扫描表目录，查找所有 .sst 文件（包括分区目录）
        collectSSTFiles(tableDir, tableDir, entries);
        
        return entries;
    }
    
    /**
     * 递归收集 SSTable 文件
     * @param scanDir 要扫描的目录
     * @param baseDir 基准目录（用于计算相对路径）
     * @param entries 收集的条目列表
     */
    private void collectSSTFiles(Path scanDir, Path baseDir, List<ManifestEntry> entries) throws IOException {
        try (java.util.stream.Stream<Path> stream = Files.walk(scanDir)) {
            stream.filter(path -> path.toString().endsWith(".sst"))
                .forEach(sstPath -> {
                    try {
                        long fileSize = Files.size(sstPath);
                        // 计算相对于表目录的相对路径
                        String relativePath = baseDir.relativize(sstPath).toString();
                        
                        ManifestEntry entry = ManifestEntry.addFile(
                            relativePath,  // 使用相对路径，如：dt=2024-01-01/data-0-000.sst
                            fileSize,
                            0,
                            null,
                            null,
                            0,
                            0
                        );
                        entries.add(entry);
                    } catch (IOException e) {
                        logger.warn("Failed to create manifest entry for {}", sstPath, e);
                    }
                });
        }
    }
    
    /**
     * 中止提交（清理临时文件）
     */
    public void abort() {
        logger.info("Aborting commit for table {}", identifier);
    }
}
