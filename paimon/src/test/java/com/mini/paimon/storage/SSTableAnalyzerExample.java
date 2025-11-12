package com.mini.paimon.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SSTable 分析器示例程序
 * 演示如何使用 SSTableAnalyzer 解析和展示 SSTable 文件内容
 */
public class SSTableAnalyzerExample {
    private static final Logger logger = LoggerFactory.getLogger(SSTableAnalyzerExample.class);

    public static void main(String[] args) {
        logger.info("=== SSTable Analyzer Example ===");

        try {
            // 1. 查找生成的 SSTable 文件
            String sstablePath = "./warehouse/example_db/user_table/data/data-0-000.sst";
            logger.info("SSTable file path: {}", sstablePath);

            // 2. 使用分析器解析 SSTable 文件
            logger.info("\n5. Analyzing SSTable file...");
            SSTableAnalyzer analyzer = new SSTableAnalyzer();
            String analysisResult = analyzer.analyzeSSTable(sstablePath);
            
            logger.info("SSTable Structure:");
            System.out.println(analysisResult);

            logger.info("\n=== SSTable Analyzer Example Completed Successfully ===");

        } catch (Exception e) {
            logger.error("Error in SSTable analyzer example", e);
        }
    }
}