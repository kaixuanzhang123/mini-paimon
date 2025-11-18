package com.mini.paimon.table;

import com.mini.paimon.index.IndexFileManager;
import com.mini.paimon.index.IndexSelector;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.schema.Schema;
import com.mini.paimon.snapshot.Snapshot;
import com.mini.paimon.snapshot.SnapshotManager;
import com.mini.paimon.utils.PathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Table Scan
 * 负责扫描表的快照，获取需要读取的数据文件列表
 * 参考 Paimon 的 DataTableScan 设计
 */
public class DataTableScan {
    private static final Logger logger = LoggerFactory.getLogger(DataTableScan.class);
    
    private final PathFactory pathFactory;
    private final String database;
    private final String table;
    private final SnapshotManager snapshotManager;
    private final Schema schema;
    private final IndexFileManager indexFileManager;
    
    private Long snapshotId;
    private Predicate predicate;
    
    public DataTableScan(PathFactory pathFactory, String database, String table, Schema schema) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.schema = schema;
        this.snapshotManager = new SnapshotManager(pathFactory, database, table);
        this.indexFileManager = new IndexFileManager(pathFactory);
    }
    
    /**
     * 指定读取的快照ID
     */
    public DataTableScan withSnapshot(long snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }
    
    /**
     * 设置谓词过滤条件
     */
    public DataTableScan withPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }
    
    /**
     * 执行扫描，获取需要读取的数据文件
     */
    public Plan plan() throws IOException {
        // 1. 确定要读取的快照
        Snapshot snapshot;
        if (snapshotId != null) {
            snapshot = snapshotManager.getSnapshot(snapshotId);
        } else {
            // 使用最新快照
            if (!snapshotManager.hasSnapshot()) {
                // 表为空，返回空计划
                return new Plan(null, new ArrayList<>());
            }
            snapshot = snapshotManager.getLatestSnapshot();
        }
        
        logger.debug("Scanning snapshot {} for table {}/{}", 
                    snapshot.getId(), database, table);
        
        // 2. 从快照中读取 ManifestList
        // 需要读取 base + delta 来获取所有数据文件
        List<DataFileMeta> dataFileMetas = new ArrayList<>();
        long snapshotIdValue = snapshot.getId();
        
        // 2.1 读取 base manifest list (如果存在)
        String baseManifestListName = snapshot.getBaseManifestList();
        if (baseManifestListName != null && !baseManifestListName.isEmpty()) {
            // 从manifest list文件名中提取正确的快照ID
            long baseSnapshotId = extractSnapshotIdFromManifestList(baseManifestListName);
            if (baseSnapshotId < 0) {
                baseSnapshotId = snapshotIdValue; // fallback
            }
            
            logger.debug("Loading base manifest list: {} with snapshot ID: {}", 
                        baseManifestListName, baseSnapshotId);
            
            ManifestList baseManifestList;
            if (baseManifestListName.startsWith("manifest-list-base-")) {
                baseManifestList = ManifestList.loadBase(pathFactory, database, table, baseSnapshotId);
            } else if (baseManifestListName.startsWith("manifest-list-delta-")) {
                // 如果base指向delta文件（首次提交的情况）
                baseManifestList = ManifestList.loadDelta(pathFactory, database, table, baseSnapshotId);
            } else {
                // 兼容旧格式
                baseManifestList = ManifestList.load(pathFactory, database, table, baseSnapshotId);
            }
            
            // 读取 base manifest 中的数据文件
            for (ManifestFileMeta manifestFileMeta : baseManifestList.getManifestFiles()) {
                String manifestFileName = manifestFileMeta.getFileName();
                String manifestId = manifestFileName.substring("manifest-".length());
                ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
                
                for (ManifestEntry entry : manifest.getEntries()) {
                    if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                        dataFileMetas.add(entry.getFile());
                    }
                }
            }
        }
        
        // 2.2 读取 delta manifest list (如果存在且与base不同)
        String deltaManifestListName = snapshot.getDeltaManifestList();
        if (deltaManifestListName != null && !deltaManifestListName.isEmpty() 
            && !deltaManifestListName.equals(baseManifestListName)) {
            ManifestList deltaManifestList = ManifestList.loadDelta(pathFactory, database, table, snapshotIdValue);
            
            // 读取 delta manifest 中的数据文件
            for (ManifestFileMeta manifestFileMeta : deltaManifestList.getManifestFiles()) {
                String manifestFileName = manifestFileMeta.getFileName();
                String manifestId = manifestFileName.substring("manifest-".length());
                ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
                
                for (ManifestEntry entry : manifest.getEntries()) {
                    if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                        dataFileMetas.add(entry.getFile());
                    }
                }
            }
        }
        
        // 如果既没有 base 也没有 delta，抛出异常
        if (baseManifestListName == null && deltaManifestListName == null) {
            throw new IOException("No manifest list found in snapshot " + snapshot.getId());
        }
        
        // 3. 使用索引过滤数据文件
        if (predicate != null) {
            IndexSelector indexSelector = new IndexSelector(indexFileManager, database, table);
            dataFileMetas = indexSelector.filterWithIndex(dataFileMetas, predicate);
        }
        
        // 4. 转换为 ManifestEntry 列表
        List<ManifestEntry> dataFiles = new ArrayList<>();
        for (DataFileMeta fileMeta : dataFileMetas) {
            dataFiles.add(new ManifestEntry(ManifestEntry.FileKind.ADD, 0, fileMeta));
        }
        
        logger.info("Scan plan generated: {} data files from snapshot {}", 
                   dataFiles.size(), snapshot.getId());
        
        return new Plan(snapshot, dataFiles);
    }
    
    /**
     * 从manifest list文件名中提取快照ID
     * 例如：manifest-list-delta-123 -> 123
     */
    private long extractSnapshotIdFromManifestList(String manifestListName) {
        try {
            if (manifestListName.startsWith("manifest-list-delta-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-delta-".length()));
            } else if (manifestListName.startsWith("manifest-list-base-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-base-".length()));
            } else if (manifestListName.startsWith("manifest-list-")) {
                return Long.parseLong(manifestListName.substring("manifest-list-".length()));
            }
        } catch (NumberFormatException e) {
            logger.warn("Failed to extract snapshot ID from manifest list: {}", manifestListName, e);
        }
        return -1;
    }
    
    /**
     * 扫描计划
     * 包含快照信息和需要读取的数据文件列表
     */
    public static class Plan {
        private final Snapshot snapshot;
        private final List<ManifestEntry> dataFiles;
        
        public Plan(Snapshot snapshot, List<ManifestEntry> dataFiles) {
            this.snapshot = snapshot;
            this.dataFiles = dataFiles;
        }
        
        public Snapshot getSnapshot() {
            return snapshot;
        }
        
        public List<ManifestEntry> getDataFiles() {
            return dataFiles;
        }
        
        public boolean isEmpty() {
            return dataFiles.isEmpty();
        }
    }
}
