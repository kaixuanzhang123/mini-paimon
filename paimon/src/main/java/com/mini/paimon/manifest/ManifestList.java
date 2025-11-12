package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest List
 * 包含多个 Manifest 文件的元信息
 * 参考 Paimon 的 ManifestList 设计
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestList {
    /** Manifest 文件元信息列表 */
    private final List<ManifestFileMeta> manifestFiles;

    @JsonCreator
    public ManifestList(@JsonProperty("manifestFiles") List<ManifestFileMeta> manifestFiles) {
        this.manifestFiles = new ArrayList<>(Objects.requireNonNull(manifestFiles, "Manifest files cannot be null"));
    }

    public ManifestList() {
        this.manifestFiles = new ArrayList<>();
    }

    /**
     * 添加 Manifest 文件元信息
     */
    public void addManifestFile(ManifestFileMeta manifestFileMeta) {
        this.manifestFiles.add(Objects.requireNonNull(manifestFileMeta, "Manifest file meta cannot be null"));
    }
    
    /**
     * 添加 Manifest 文件路径（兼容旧接口）
     * @deprecated 使用 addManifestFile(ManifestFileMeta) 替代
     */
    @Deprecated
    public void addManifestFile(String manifestFile) {
        // 为了向后兼容，创建一个简单的 ManifestFileMeta
        ManifestFileMeta meta = new ManifestFileMeta(
            manifestFile, 0, 0, 0, 0, null, null
        );
        this.manifestFiles.add(meta);
    }

    /**
     * 获取所有 Manifest 文件元信息
     */
    public List<ManifestFileMeta> getManifestFiles() {
        return Collections.unmodifiableList(manifestFiles);
    }

    /**
     * 获取 Manifest 文件数量
     */
    public int size() {
        return manifestFiles.size();
    }

    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return manifestFiles.isEmpty();
    }

    /**
     * 持久化 Manifest List
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param snapshotId 快照ID
     * @throws IOException 序列化异常
     */
    public void persist(PathFactory pathFactory, String database, String table, long snapshotId) throws IOException {
        Path manifestListPath = pathFactory.getManifestListPath(database, table, snapshotId);
        Files.createDirectories(manifestListPath.getParent());
        SerializationUtils.writeToFile(manifestListPath, this);
    }

    /**
     * 从文件加载 Manifest List
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param snapshotId 快照ID
     * @return Manifest List
     * @throws IOException 反序列化异常
     */
    public static ManifestList load(PathFactory pathFactory, String database, String table, long snapshotId) throws IOException {
        Path manifestListPath = pathFactory.getManifestListPath(database, table, snapshotId);
        if (!Files.exists(manifestListPath)) {
            throw new IOException("Manifest list file not found: " + manifestListPath);
        }
        return SerializationUtils.readFromFile(manifestListPath, ManifestList.class);
    }

    /**
     * 检查 Manifest List 文件是否存在
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param snapshotId 快照ID
     * @return true 如果存在，否则 false
     */
    public static boolean exists(PathFactory pathFactory, String database, String table, long snapshotId) {
        try {
            Path manifestListPath = pathFactory.getManifestListPath(database, table, snapshotId);
            return Files.exists(manifestListPath);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestList that = (ManifestList) o;
        return manifestFiles.equals(that.manifestFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(manifestFiles);
    }

    @Override
    public String toString() {
        return "ManifestList{" +
                "manifestFiles=" + manifestFiles +
                '}';
    }
}
