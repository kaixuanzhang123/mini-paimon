package com.minipaimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.minipaimon.utils.PathFactory;
import com.minipaimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest List
 * 记录一组 Manifest 文件的路径
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestList {
    /** Manifest 文件路径列表 */
    private final List<String> manifestFiles;

    @JsonCreator
    public ManifestList(@JsonProperty("manifestFiles") List<String> manifestFiles) {
        this.manifestFiles = new ArrayList<>(Objects.requireNonNull(manifestFiles, "Manifest files cannot be null"));
    }

    public ManifestList() {
        this.manifestFiles = new ArrayList<>();
    }

    /**
     * 添加 Manifest 文件路径
     */
    public void addManifestFile(String manifestFile) {
        this.manifestFiles.add(Objects.requireNonNull(manifestFile, "Manifest file cannot be null"));
    }

    /**
     * 获取所有 Manifest 文件路径
     */
    public List<String> getManifestFiles() {
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
