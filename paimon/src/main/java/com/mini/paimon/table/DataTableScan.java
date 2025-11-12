package com.mini.paimon.table;

import com.mini.paimon.index.IndexFileManager;
import com.mini.paimon.index.IndexSelector;
import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.manifest.ManifestFile;
import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.manifest.ManifestList;
import com.mini.paimon.manifest.DataFileMeta;
import com.mini.paimon.metadata.Schema;
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
                    snapshot.getSnapshotId(), database, table);
        
        // 2. 从快照中读取 ManifestList
        ManifestList manifestList = ManifestList.load(
            pathFactory, database, table, snapshot.getSnapshotId());
        
        // 3. 读取所有 Manifest 文件，获取数据文件列表
        List<DataFileMeta> dataFileMetas = new ArrayList<>();
        for (ManifestFileMeta manifestFileMeta : manifestList.getManifestFiles()) {
            // 从文件名中提取ID
            String manifestFileName = manifestFileMeta.getFileName();
            String manifestId = manifestFileName.substring("manifest-".length());
            ManifestFile manifest = ManifestFile.load(pathFactory, database, table, manifestId);
            
            // 只收集 ADD 类型的文件
            for (ManifestEntry entry : manifest.getEntries()) {
                if (entry.getKind() == ManifestEntry.FileKind.ADD) {
                    dataFileMetas.add(entry.getFile());
                }
            }
        }
        
        // 4. 使用索引过滤数据文件
        if (predicate != null) {
            IndexSelector indexSelector = new IndexSelector(indexFileManager, database, table);
            dataFileMetas = indexSelector.filterWithIndex(dataFileMetas, predicate);
        }
        
        // 5. 转换为 ManifestEntry 列表
        List<ManifestEntry> dataFiles = new ArrayList<>();
        for (DataFileMeta fileMeta : dataFileMetas) {
            dataFiles.add(new ManifestEntry(ManifestEntry.FileKind.ADD, 0, fileMeta));
        }
        
        logger.info("Scan plan generated: {} data files from snapshot {}", 
                   dataFiles.size(), snapshot.getSnapshotId());
        
        return new Plan(snapshot, dataFiles);
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
