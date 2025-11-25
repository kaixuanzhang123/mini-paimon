package com.mini.paimon.manifest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest File (纯数据类)
 * 参考 Apache Paimon 的 ManifestFile 设计
 * 注意：此类只负责存储数据，不包含任何 I/O 操作。
 * I/O 操作请使用 {@link com.mini.paimon.io.ManifestFileIO}
 * 
 * @see com.mini.paimon.io.ManifestFileIO 用于读写 ManifestFile 的 I/O 类
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ManifestFile {
    
    private final List<ManifestEntry> entries;

    @JsonCreator
    public ManifestFile(@JsonProperty("entries") List<ManifestEntry> entries) {
        this.entries = new ArrayList<>(Objects.requireNonNull(entries, "Entries cannot be null"));
    }

    public ManifestFile() {
        this.entries = new ArrayList<>();
    }

    public void addEntry(ManifestEntry entry) {
        this.entries.add(Objects.requireNonNull(entry, "Entry cannot be null"));
    }

    public List<ManifestEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
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
        return "ManifestFile{entries=" + entries.size() + "}";
    }
}
