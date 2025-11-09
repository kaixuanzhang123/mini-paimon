package com.mini.paimon.storage;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 分区 LSMTree
 * 继承 LSMTree，将数据写入到指定的分区目录
 * 参考 Paimon 实现，每个分区有独立的数据目录
 */
public class PartitionedLSMTree extends LSMTree {
    
    private final PartitionSpec partitionSpec;
    private final Path partitionDataDir;
    
    public PartitionedLSMTree(Schema schema, PathFactory pathFactory, 
                             String database, String table, 
                             PartitionSpec partitionSpec) throws IOException {
        // 调用父类构造函数，禁用自动快照（由 TableCommit 统一管理）
        super(schema, new PartitionedPathFactory(pathFactory, database, table, partitionSpec), 
              database, table, false);  // false 表示禁用自动快照
        
        this.partitionSpec = partitionSpec;
        
        // 计算分区数据目录：table/dt=2024-01-01（与 snapshot、manifest 同级）
        Path tableDir = pathFactory.getTablePath(database, table);
        this.partitionDataDir = tableDir.resolve(partitionSpec.toPath());
        
        // 确保分区目录存在
        Files.createDirectories(partitionDataDir);
    }
    
    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }
    
    /**
     * 分区专用的 PathFactory 包装器
     * 将数据文件路径重定向到表目录下的分区子目录（与 snapshot、manifest 同级）
     */
    private static class PartitionedPathFactory extends PathFactory {
        private final PathFactory delegate;
        private final String database;
        private final String table;
        private final PartitionSpec partitionSpec;
        private final String partitionPath;
        
        public PartitionedPathFactory(PathFactory delegate, String database, 
                                     String table, PartitionSpec partitionSpec) {
            super(delegate.getWarehousePath());
            this.delegate = delegate;
            this.database = database;
            this.table = table;
            this.partitionSpec = partitionSpec;
            this.partitionPath = partitionSpec.toPath();
        }
        
        @Override
        public Path getDataDir(String database, String table) {
            // 返回分区目录：table/dt=2024-01-01（与 snapshot、manifest 同级）
            Path tableDir = delegate.getTablePath(database, table);
            return tableDir.resolve(partitionPath);
        }
        
        @Override
        public Path getWalDir(String database, String table) {
            // WAL 也按分区存储在分区目录下
            Path tableDir = delegate.getTablePath(database, table);
            return tableDir.resolve(partitionPath).resolve("wal");
        }
        
        @Override
        public Path getSSTPath(String database, String table, int level, long sequence) {
            // SSTable 文件直接存储在分区目录下
            Path partitionDir = getDataDir(database, table);
            return partitionDir.resolve(String.format("data-%d-%03d.sst", level, sequence));
        }
        
        @Override
        public Path getWalPath(String database, String table, long sequence) {
            Path walDir = getWalDir(database, table);
            return walDir.resolve(String.format("wal-%03d.log", sequence));
        }
        
        @Override
        public void createTableDirectories(String database, String table) throws IOException {
            // 创建分区目录结构
            Files.createDirectories(getDataDir(database, table));
            Files.createDirectories(getWalDir(database, table));
        }
        
        // 其他方法委托给原始 PathFactory
        @Override
        public Path getTablePath(String database, String table) {
            return delegate.getTablePath(database, table);
        }
        
        @Override
        public Path getSchemaDir(String database, String table) {
            return delegate.getSchemaDir(database, table);
        }
        
        @Override
        public Path getSnapshotDir(String database, String table) {
            return delegate.getSnapshotDir(database, table);
        }
        
        @Override
        public Path getManifestDir(String database, String table) {
            return delegate.getManifestDir(database, table);
        }
    }
}
