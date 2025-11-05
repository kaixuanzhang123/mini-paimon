package com.minipaimon.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 序列化工具类
 * 提供JSON序列化和反序列化功能
 */
public class SerializationUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        // 配置ObjectMapper
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        OBJECT_MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * 获取ObjectMapper实例
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * 将对象序列化为JSON字符串
     */
    public static String toJson(Object object) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(object);
    }

    /**
     * 从JSON字符串反序列化对象
     */
    public static <T> T fromJson(String json, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(json, clazz);
    }

    /**
     * 将对象序列化到文件
     */
    public static void writeToFile(Path path, Object object) throws IOException {
        Files.createDirectories(path.getParent());
        OBJECT_MAPPER.writeValue(path.toFile(), object);
    }

    /**
     * 从文件反序列化对象
     */
    public static <T> T readFromFile(Path path, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(path.toFile(), clazz);
    }

    /**
     * 将对象序列化为字节数组
     */
    public static byte[] toBytes(Object object) throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(object);
    }

    /**
     * 从字节数组反序列化对象
     */
    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) throws IOException {
        return OBJECT_MAPPER.readValue(bytes, clazz);
    }
}
