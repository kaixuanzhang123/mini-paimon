package com.mini.paimon.storage;

import com.mini.paimon.utils.PathFactory;
import com.mini.paimon.schema.DataType;
import com.mini.paimon.schema.Field;
import com.mini.paimon.schema.Row;
import com.mini.paimon.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * LSM Tree 高级特性示例
 * 演示：1. 非主键表支持 2. WAL恢复 3. Compaction
 */
public class AdvancedFeaturesExample {
    private static final Logger logger = LoggerFactory.getLogger(AdvancedFeaturesExample.class);
    
    public static void main(String[] args) throws Exception {
        // 示例1: 非主键表
        demonstrateNoPrimaryKeyTable();
        
        // 示例2: WAL恢复
        demonstrateWALRecovery();
        
        // 示例3: Compaction
        demonstrateCompaction();
    }
    
    /**
     * 示例1: 非主键表支持
     */
    private static void demonstrateNoPrimaryKeyTable() throws Exception {
        logger.info("=== 示例1: 非主键表 ===");
        
        PathFactory pathFactory = new PathFactory("warehouse");
        
        // 创建无主键的Schema
        Schema noPkSchema = new Schema(
            1,
            Arrays.asList(
                new Field("name", DataType.STRING(), true),
                new Field("value", DataType.INT(), true)
            ),
            Arrays.asList(), // 空主键列表
            Arrays.asList()
        );
        
        LSMTree lsmTree = new LSMTree(noPkSchema, pathFactory, "demo_db", "no_pk_table");
        
        // 插入数据 - 可以有重复的name
        lsmTree.put(new Row(new Object[]{"item1", 100}));
        lsmTree.put(new Row(new Object[]{"item2", 200}));
        lsmTree.put(new Row(new Object[]{"item1", 150})); // name重复也可以
        
        List<Row> rows = lsmTree.scan();
        logger.info("非主键表共有 {} 行数据", rows.size());
        
        lsmTree.close();
        logger.info("非主键表示例完成\n");
    }
    
    /**
     * 示例2: WAL恢复
     */
    private static void demonstrateWALRecovery() throws Exception {
        logger.info("=== 示例2: WAL恢复 ===");
        
        PathFactory pathFactory = new PathFactory("warehouse");
        
        Schema schema = new Schema(
            2,
            Arrays.asList(
                new Field("id", DataType.INT(), false),
                new Field("name", DataType.STRING(), true)
            ),
            Arrays.asList("id"),
            Arrays.asList()
        );
        
        // 第一次打开，插入数据
        LSMTree lsmTree1 = new LSMTree(schema, pathFactory, "demo_db", "wal_table");
        lsmTree1.put(new Row(new Object[]{1, "Alice"}));
        lsmTree1.put(new Row(new Object[]{2, "Bob"}));
        logger.info("插入2条数据");
        lsmTree1.close();
        
        // 重新打开，WAL会自动恢复
        LSMTree lsmTree2 = new LSMTree(schema, pathFactory, "demo_db", "wal_table");
        List<Row> recoveredRows = lsmTree2.scan();
        logger.info("从WAL恢复了 {} 行数据", recoveredRows.size());
        
        lsmTree2.close();
        logger.info("WAL恢复示例完成\n");
    }
    
    /**
     * 示例3: Compaction
     */
    private static void demonstrateCompaction() throws Exception {
        logger.info("=== 示例3: Compaction ===");
        
        PathFactory pathFactory = new PathFactory("warehouse");
        
        Schema schema = new Schema(
            3,
            Arrays.asList(
                new Field("id", DataType.INT(), false),
                new Field("data", DataType.STRING(), true)
            ),
            Arrays.asList("id"),
            Arrays.asList()
        );
        
        LSMTree lsmTree = new LSMTree(schema, pathFactory, "demo_db", "compaction_table");
        
        // 插入大量数据触发多次flush，可能触发compaction
        logger.info("开始插入数据...");
        for (int i = 0; i < 50; i++) {
            lsmTree.put(new Row(new Object[]{i, "data_" + i}));
        }
        logger.info("插入了50条数据");
        
        // 验证数据
        List<Row> allRows = lsmTree.scan();
        logger.info("扫描得到 {} 行数据", allRows.size());
        
        lsmTree.close();
        logger.info("Compaction示例完成\n");
    }
}
