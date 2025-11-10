package com.mini.paimon.storage;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 支持 Bucket 的分区 LSMTree
 * 参考 Paimon 实现，每个分区的每个 Bucket 有独立的数据目录
 * 
 * 目录结构：
 * - 数据文件：table/dt=2024-01-01/bucket-0/*.sst
 * - WAL 文件：table/wal/wal-bucket-0-000.log（表级 WAL 目录，使用 bucket 前缀区分）
 */
public class BucketedLSMTree extends LSMTree {
    
    private final PartitionSpec partitionSpec;
    private final int bucket;
    private final Path bucketDataDir;
    
    public BucketedLSMTree(Schema schema, PathFactory pathFactory, 
                          String database, String table, 
                          PartitionSpec partitionSpec,
                          int bucket,
                          int totalBuckets) throws IOException {
        // 调用父类构造函数，使用自定义的 PathFactory
        // 关键：禁用自动快照，不加载现有文件（因为分区表的文件由 Snapshot 管理）
        super(schema, new BucketedPathFactory(pathFactory, database, table, partitionSpec, bucket), 
              database, table, false, false);  // autoSnapshot=false, loadExistingFiles=false
        
        this.partitionSpec = partitionSpec;
        this.bucket = bucket;
        
        // 计算 Bucket 数据目录：table/dt=2024-01-01/bucket-0
        Path tableDir = pathFactory.getTablePath(database, table);
        this.bucketDataDir = tableDir
            .resolve(partitionSpec.toPath())
            .resolve("bucket-" + bucket);
        
        // 确保 Bucket 目录存在
        Files.createDirectories(bucketDataDir);
        
        // 关键修复：删除过期的 WAL 文件，避免恢复旧数据
        // 在 OVERWRITE 提交前，WAL 文件应该被清空
        cleanupStaleWALFiles(pathFactory, database, table, partitionSpec.toPath(), bucket);
    }
    
    /**
     * 清理过期的 WAL 文件
     * 在创建 BucketedLSMTree 前调用，确保不会从旧 WAL 恢复数据
     */
    private static void cleanupStaleWALFiles(PathFactory pathFactory, String database, 
                                            String table, String partitionPath, int bucket) throws IOException {
        try {
            java.nio.file.Path walDir = pathFactory.getWalDir(database, table);
            if (java.nio.file.Files.exists(walDir)) {
                String walPrefix = "wal-" + partitionPath.replace("/", "-") + "-bucket-" + bucket;
                try (java.util.stream.Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(walDir, 1)) {
                    stream.filter(path -> {
                        String fileName = path.getFileName().toString();
                        return fileName.startsWith(walPrefix) && fileName.endsWith(".log");
                    }).forEach(path -> {
                        try {
                            java.nio.file.Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // 忽略错误
                        }
                    });
                }
            }
        } catch (IOException e) {
            // 忽略错误，不影响正常流程
        }
    }
    
    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }
    
    public int getBucket() {
        return bucket;
    }
    
    /**
     * Bucket 专用的 PathFactory 包装器
     * 将数据文件路径重定向到 bucket 子目录
     */
    private static class BucketedPathFactory extends PathFactory {
        private final PathFactory delegate;
        private final String database;
        private final String table;
        private final PartitionSpec partitionSpec;
        private final int bucket;
        private final String partitionPath;
        
        public BucketedPathFactory(PathFactory delegate, String database, 
                                  String table, PartitionSpec partitionSpec, int bucket) {
            super(delegate.getWarehousePath());
            this.delegate = delegate;
            this.database = database;
            this.table = table;
            this.partitionSpec = partitionSpec;
            this.bucket = bucket;
            this.partitionPath = partitionSpec.toPath();
        }
        
        @Override
        public Path getDataDir(String database, String table) {
            // 返回 Bucket 目录：table/dt=2024-01-01/bucket-0
            Path tableDir = delegate.getTablePath(database, table);
            return tableDir.resolve(partitionPath).resolve("bucket-" + bucket);
        }
        
        @Override
        public Path getWalDir(String database, String table) {
            // WAL 是表级别的，不是 Bucket 级别（与 Paimon 对齐）
            return delegate.getWalDir(database, table);
        }
        
        @Override
        public Path getSSTPath(String database, String table, int level, long sequence) {
            // SSTable 文件存储在 Bucket 目录下
            Path bucketDir = getDataDir(database, table);
            return bucketDir.resolve(String.format("data-%d-%03d.sst", level, sequence));
        }
        
        @Override
        public Path getWalPath(String database, String table, long sequence) {
            // WAL 文件需要包含分区信息，避免不同分区的同一 bucket 共享 WAL
            // 格式：wal-{partitionPath}-bucket-{bucketId}-{sequence}.log
            // 例如：wal-dt=2024-01-01-bucket-3-000.log
            Path walDir = delegate.getWalDir(database, table);
            String walFileName = String.format("wal-%s-bucket-%d-%03d.log", 
                partitionPath.replace("/", "-"), bucket, sequence);
            return walDir.resolve(walFileName);
        }
        
        @Override
        public void createTableDirectories(String database, String table) throws IOException {
            // 只创建 Bucket 数据目录，WAL 目录由表级统一管理
            Files.createDirectories(getDataDir(database, table));
            // 不再创建 Bucket 级的 WAL 目录
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
