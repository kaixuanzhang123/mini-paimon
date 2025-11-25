package com.mini.paimon.io;

import com.mini.paimon.manifest.ManifestFileMeta;
import com.mini.paimon.utils.IdGenerator;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Manifest List IO
 * 基于ObjectsFile的ManifestList读写实现
 * 参考Apache Paimon的设计，提供统一的序列化接口
 */
public class ManifestListIO extends ObjectsFile<ManifestFileMeta> {
    
    private final IdGenerator idGenerator;
    
    public ManifestListIO(PathFactory pathFactory, String database, String table) {
        this(pathFactory, database, table, 8 * 1024 * 1024L); // 默认8MB
    }
    
    public ManifestListIO(
            PathFactory pathFactory, 
            String database, 
            String table,
            long suggestedFileSize) {
        super(pathFactory, database, table, ManifestFileMeta.class, suggestedFileSize);
        this.idGenerator = new IdGenerator();
    }
    
    /**
     * 写入base manifest list
     * 
     * @param metas manifest file meta列表
     * @param snapshotId snapshot ID
     * @return 文件名
     * @throws IOException 写入失败
     */
    public String writeBaseManifestList(List<ManifestFileMeta> metas, long snapshotId) 
            throws IOException {
        String fileName = "manifest-list-base-" + snapshotId;
        write(metas, fileName);
        return fileName;
    }
    
    /**
     * 写入delta manifest list
     * 
     * @param metas manifest file meta列表
     * @param snapshotId snapshot ID
     * @return 文件名
     * @throws IOException 写入失败
     */
    public String writeDeltaManifestList(List<ManifestFileMeta> metas, long snapshotId) 
            throws IOException {
        String fileName = "manifest-list-delta-" + snapshotId;
        write(metas, fileName);
        return fileName;
    }
    
    /**
     * 读取manifest list
     * 
     * @param fileName 文件名（可以是完整名或简化名）
     * @return manifest file meta列表
     * @throws IOException 读取失败
     */
    public List<ManifestFileMeta> readManifestList(String fileName) throws IOException {
        return read(fileName);
    }
    
    /**
     * 读取base manifest list
     */
    public List<ManifestFileMeta> readBaseManifestList(long snapshotId) throws IOException {
        return readManifestList("manifest-list-base-" + snapshotId);
    }
    
    /**
     * 读取delta manifest list
     */
    public List<ManifestFileMeta> readDeltaManifestList(long snapshotId) throws IOException {
        return readManifestList("manifest-list-delta-" + snapshotId);
    }
    
    @Override
    protected Path getFilePath(String fileName) {
        // manifest list 文件存储在 manifest 目录下
        if (fileName.startsWith("manifest-list-")) {
            return pathFactory.getManifestDir(database, table).resolve(fileName);
        }
        // 否则作为manifest list ID处理
        return pathFactory.getManifestDir(database, table).resolve("manifest-list-" + fileName);
    }
    
    @Override
    protected String generateFileName() {
        return "manifest-list-" + idGenerator.generateManifestId();
    }
}

