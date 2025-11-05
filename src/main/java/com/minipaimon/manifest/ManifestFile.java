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
 * Manifest 文件
 * 包含一组 Manifest 条目
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestFile {
    /** Manifest 条目列表 */
    private final List<ManifestEntry> entries;

    @JsonCreator
    public ManifestFile(@JsonProperty("entries") List<ManifestEntry> entries) {
        this.entries = new ArrayList<>(Objects.requireNonNull(entries, "Entries cannot be null"));
    }

    public ManifestFile() {
        this.entries = new ArrayList<>();
    }

    /**
     * 添加条目
     */
    public void addEntry(ManifestEntry entry) {
        this.entries.add(Objects.requireNonNull(entry, "Entry cannot be null"));
    }

    /**
     * 获取所有条目
     */
    public List<ManifestEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    /**
     * 获取条目数量
     */
    public int size() {
        return entries.size();
    }

    /**
     * 检查是否为空
     */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /**
     * 持久化 Manifest 文件
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param manifestId Manifest ID
     * @throws IOException 序列化异常
     */
    public void persist(PathFactory pathFactory, String database, String table, String manifestId) throws IOException {
        Path manifestPath = pathFactory.getManifestPath(database, table, manifestId);
        Files.createDirectories(manifestPath.getParent());
        SerializationUtils.writeToFile(manifestPath, this);
    }

    /**
     * 从文件加载 Manifest
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param manifestId Manifest ID
     * @return Manifest 文件
     * @throws IOException 反序列化异常
     */
    public static ManifestFile load(PathFactory pathFactory, String database, String table, String manifestId) throws IOException {
        Path manifestPath = pathFactory.getManifestPath(database, table, manifestId);
        if (!Files.exists(manifestPath)) {
            throw new IOException("Manifest file not found: " + manifestPath);
        }
        return SerializationUtils.readFromFile(manifestPath, ManifestFile.class);
    }

    /**
     * 检查 Manifest 文件是否存在
     * 
     * @param pathFactory 路径工厂
     * @param database 数据库名
     * @param table 表名
     * @param manifestId Manifest ID
     * @return true 如果存在，否则 false
     */
    public static boolean exists(PathFactory pathFactory, String database, String table, String manifestId) {
        try {
            Path manifestPath = pathFactory.getManifestPath(database, table, manifestId);
            return Files.exists(manifestPath);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ManifestFile that = (ManifestFile) o;
        return entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entries);
    }

    @Override
    public String toString() {
        return "ManifestFile{" +
                "entries=" + entries +
                '}';
    }
}