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
        
        if (manifestEntries.isEmpty()) {
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
     */
    private List<ManifestEntry> collectManifestEntries() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        
        Path dataDir = pathFactory.getDataDir(identifier.getDatabase(), identifier.getTable());
        if (!Files.exists(dataDir)) {
            return entries;
        }
        
        try (java.util.stream.Stream<Path> stream = Files.list(dataDir)) {
            stream.filter(path -> path.toString().endsWith(".sst"))
                .forEach(sstPath -> {
                    try {
                        long fileSize = Files.size(sstPath);
                        ManifestEntry entry = ManifestEntry.addFile(
                            sstPath.toString(),
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
        
        return entries;
    }
    
    /**
     * 中止提交（清理临时文件）
     */
    public void abort() {
        logger.info("Aborting commit for table {}", identifier);
    }
}
