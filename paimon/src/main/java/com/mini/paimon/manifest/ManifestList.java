package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest List (纯数据类)
 * 参考 Apache Paimon 的 ManifestList 设计
 * 注意：此类只负责存储数据，不包含任何 I/O 操作。
 * I/O 操作请使用 {@link com.mini.paimon.io.ManifestListIO}
 * 
 * @see com.mini.paimon.io.ManifestListIO 用于读写 ManifestList 的 I/O 类
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestList {
    
    private final List<ManifestFileMeta> manifestFiles;

    @JsonCreator
    public ManifestList(@JsonProperty("manifestFiles") List<ManifestFileMeta> manifestFiles) {
        this.manifestFiles = new ArrayList<>(Objects.requireNonNull(manifestFiles, "Manifest files cannot be null"));
    }

    public ManifestList() {
        this.manifestFiles = new ArrayList<>();
    }

    public void addManifestFile(ManifestFileMeta manifestFileMeta) {
        this.manifestFiles.add(Objects.requireNonNull(manifestFileMeta, "Manifest file meta cannot be null"));
    }

    public List<ManifestFileMeta> getManifestFiles() {
        return Collections.unmodifiableList(manifestFiles);
    }

    public int size() {
        return manifestFiles.size();
    }

    public boolean isEmpty() {
        return manifestFiles.isEmpty();
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
        return "ManifestList{manifestFiles=" + manifestFiles.size() + "}";
    }
}
