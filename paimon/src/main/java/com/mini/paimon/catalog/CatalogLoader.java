package com.mini.paimon.catalog;

import com.mini.paimon.exception.CatalogException;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Catalog 加载器
 * 参考 Apache Paimon 的 CatalogFactory 加载机制
 * 
 * 通过 SPI (Service Provider Interface) 机制动态加载 CatalogFactory 实现
 * 支持通过标识符创建不同类型的 Catalog
 */
public class CatalogLoader {
    
    /** 缓存已加载的 CatalogFactory */
    private static final Map<String, CatalogFactory> FACTORY_CACHE = new HashMap<>();
    
    static {
        // 使用 SPI 加载所有 CatalogFactory 实现
        ServiceLoader<CatalogFactory> serviceLoader = ServiceLoader.load(CatalogFactory.class);
        for (CatalogFactory factory : serviceLoader) {
            String identifier = factory.identifier();
            if (FACTORY_CACHE.containsKey(identifier)) {
                throw new IllegalStateException(
                    "Duplicate CatalogFactory identifier: " + identifier);
            }
            FACTORY_CACHE.put(identifier, factory);
        }
    }
    
    /**
     * 根据标识符创建 Catalog
     * 
     * @param identifier Catalog 类型标识符
     * @param context Catalog 上下文配置
     * @return Catalog 实例
     * @throws CatalogException 如果找不到对应的 Factory 或创建失败
     */
    public static Catalog load(String identifier, CatalogContext context) throws CatalogException {
        CatalogFactory factory = FACTORY_CACHE.get(identifier);
        if (factory == null) {
            throw new CatalogException(
                "Cannot find CatalogFactory for identifier: " + identifier + 
                ". Available identifiers: " + FACTORY_CACHE.keySet());
        }
        
        try {
            return factory.create(context);
        } catch (Exception e) {
            throw new CatalogException(
                "Failed to create Catalog with identifier: " + identifier, e);
        }
    }
    
    /**
     * 获取所有已注册的 Catalog 类型标识符
     * 
     * @return 标识符集合
     */
    public static java.util.Set<String> getAvailableIdentifiers() {
        return FACTORY_CACHE.keySet();
    }
    
    /**
     * 检查指定标识符的 Factory 是否已注册
     * 
     * @param identifier Catalog 类型标识符
     * @return true 如果已注册，否则 false
     */
    public static boolean isFactoryRegistered(String identifier) {
        return FACTORY_CACHE.containsKey(identifier);
    }
}

