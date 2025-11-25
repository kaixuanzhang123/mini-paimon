package com.mini.paimon.io;

import com.mini.paimon.manifest.ManifestEntry;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Manifest File IO
 * 基于ObjectsFile的ManifestFile读写实现
 * 参考Apache Paimon的设计，提供统一的序列化接口
 */
public class ManifestFileIO extends ObjectsFile<ManifestEntry> {
    
    private final IdGenerator idGenerator;
    
    public ManifestFileIO(PathFactory pathFactory, String database, String table) {
        this(pathFactory, database, table, 8 * 1024 * 1024L); // 默认8MB
    }
    
    public ManifestFileIO(
            PathFactory pathFactory, 
            String database, 
            String table,
            long suggestedFileSize) {
        super(pathFactory, database, table, ManifestEntry.class, suggestedFileSize);
        this.idGenerator = new IdGenerator();
    }
    
    /**
     * 写入manifest文件
     * 
     * @param entries manifest entries
     * @param manifestId manifest ID
     * @return 文件名
     * @throws IOException 写入失败
     */
    public String writeManifest(List<ManifestEntry> entries, String manifestId) 
            throws IOException {
        String fileName = "manifest-" + manifestId;
        write(entries, fileName);
        return fileName;
    }
    
    /**
     * 写入manifest文件（自动生成ID）
     * 
     * @param entries manifest entries
     * @return 文件名
     * @throws IOException 写入失败
     */
    public String writeManifest(List<ManifestEntry> entries) throws IOException {
        String manifestId = idGenerator.generateManifestId();
        return writeManifest(entries, manifestId);
    }
    
    /**
     * 读取manifest文件
     * 
     * @param fileName 文件名（可以是完整名或简化名）
     * @return manifest entries
     * @throws IOException 读取失败
     */
    public List<ManifestEntry> readManifest(String fileName) throws IOException {
        return read(fileName);
    }
    
    /**
     * 读取manifest文件（通过ID）
     * 
     * @param manifestId manifest ID
     * @return manifest entries
     * @throws IOException 读取失败
     */
    public List<ManifestEntry> readManifestById(String manifestId) throws IOException {
        if (manifestId.startsWith("manifest-")) {
            return readManifest(manifestId);
        }
        return readManifest("manifest-" + manifestId);
    }
    
    @Override
    protected Path getFilePath(String fileName) {
        // 如果文件名已经包含完整路径信息，直接使用
        if (fileName.startsWith("manifest-")) {
            return pathFactory.getManifestDir(database, table).resolve(fileName);
        }
        // 否则作为manifest ID处理
        return pathFactory.getManifestPath(database, table, fileName);
    }
    
    @Override
    protected String generateFileName() {
        return "manifest-" + idGenerator.generateManifestId();
    }
}

