package com.mini.paimon.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.utils.SerializationUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Objects File - 对象文件读写抽象类
 * 参考Apache Paimon的ObjectsFile设计
 * 用于读写Manifest文件和ManifestList文件
 * 当前使用JSON实现，未来可以扩展为Avro格式
 * 
 * @param <T> 对象类型
 */
public abstract class ObjectsFile<T> {
    
    protected final PathFactory pathFactory;
    protected final String database;
    protected final String table;
    protected final Class<T> objectClass;
    protected final long suggestedFileSize;
    
    public ObjectsFile(PathFactory pathFactory, String database, String table,
                      Class<T> objectClass, long suggestedFileSize) {
        this.pathFactory = pathFactory;
        this.database = database;
        this.table = table;
        this.objectClass = objectClass;
        this.suggestedFileSize = suggestedFileSize;
    }
    
    /**
     * 写入对象列表到文件
     * 
     * @param objects 对象迭代器
     * @param fileName 文件名（不含路径）
     * @return 实际写入的字节数
     * @throws IOException 写入失败
     */
    public long write(Iterator<T> objects, String fileName) throws IOException {
        Path filePath = getFilePath(fileName);
        Files.createDirectories(filePath.getParent());
        
        // 收集所有对象
        List<T> objectList = new ArrayList<>();
        while (objects.hasNext()) {
            objectList.add(objects.next());
        }
        
        // 写入文件（直接序列化列表）
        SerializationUtils.writeToFile(filePath, objectList);
        
        // 返回文件大小
        return Files.size(filePath);
    }
    
    /**
     * 写入对象列表（便捷方法）
     */
    public long write(List<T> objects, String fileName) throws IOException {
        return write(objects.iterator(), fileName);
    }
    
    /**
     * 从文件读取对象列表
     * 
     * @param fileName 文件名（不含路径）
     * @return 对象列表
     * @throws IOException 读取失败
     */
    public List<T> read(String fileName) throws IOException {
        Path filePath = getFilePath(fileName);
        
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + filePath);
        }
        
        // 使用 Jackson CollectionType 正确反序列化泛型列表
        ObjectMapper mapper = SerializationUtils.getObjectMapper();
        CollectionType listType = mapper.getTypeFactory()
            .constructCollectionType(List.class, objectClass);
        
        return mapper.readValue(filePath.toFile(), listType);
    }
    
    /**
     * 写入对象并支持rolling（根据文件大小自动分割）
     * 
     * @param objects 对象列表
     * @return 写入的文件列表及其大小
     * @throws IOException 写入失败
     */
    public List<FileInfo> writeWithRolling(List<T> objects) throws IOException {
        List<FileInfo> files = new ArrayList<>();
        
        if (objects.isEmpty()) {
            return files;
        }
        
        // 简化实现：如果对象数量不多，写入单个文件
        // 实际Paimon会根据文件大小进行rolling
        String fileName = generateFileName();
        long size = write(objects, fileName);
        files.add(new FileInfo(fileName, size));
        
        return files;
    }
    
    /**
     * 获取文件路径
     */
    protected abstract Path getFilePath(String fileName);
    
    /**
     * 生成文件名
     */
    protected abstract String generateFileName();
    
    /**
     * 文件信息
     */
    public static class FileInfo {
        private final String fileName;
        private final long fileSize;
        
        public FileInfo(String fileName, long fileSize) {
            this.fileName = fileName;
            this.fileSize = fileSize;
        }
        
        public String getFileName() {
            return fileName;
        }
        
        public long getFileSize() {
            return fileSize;
        }
    }
}

