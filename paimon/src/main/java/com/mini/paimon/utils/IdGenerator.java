package com.mini.paimon.utils;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ID生成器
 * 用于生成各种ID（Snapshot ID、Sequence等）
 */
public class IdGenerator {
    private final AtomicLong counter;

    public IdGenerator() {
        this(0);
    }

    public IdGenerator(long initialValue) {
        this.counter = new AtomicLong(initialValue);
    }

    /**
     * 生成下一个ID
     */
    public long nextId() {
        return counter.incrementAndGet();
    }

    /**
     * 获取当前ID（不递增）
     */
    public long currentId() {
        return counter.get();
    }

    /**
     * 生成UUID字符串
     */
    public static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 生成Manifest文件ID
     */
    public static String generateManifestId() {
        return generateUUID().substring(0, 16);
    }
}
