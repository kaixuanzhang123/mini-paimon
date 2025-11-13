package com.mini.paimon.metadata;

import com.mini.paimon.partition.DynamicPartitionGenerator;
import com.mini.paimon.partition.PartitionSpec;
import com.mini.paimon.partition.PartitionStatistics;
import com.mini.paimon.storage.MergeEngine;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class NewFeaturesTest {
    
    @Test
    public void testComplexDataTypes() {
        DataType arrayType = DataType.ARRAY(DataType.INT());
        assertTrue(arrayType instanceof DataType.ArrayType);
        assertTrue(arrayType.isCompatible(Arrays.asList(1, 2, 3)));
        assertFalse(arrayType.isCompatible(Arrays.asList("a", "b")));
        
        DataType mapType = DataType.MAP(DataType.STRING(), DataType.INT());
        assertTrue(mapType instanceof DataType.MapType);
        Map<String, Integer> testMap = new HashMap<>();
        testMap.put("key1", 1);
        assertTrue(mapType.isCompatible(testMap));
        
        DataType timestampType = DataType.TIMESTAMP();
        assertTrue(timestampType.isCompatible(LocalDateTime.now()));
        assertTrue(timestampType.isCompatible(System.currentTimeMillis()));
        
        DataType decimalType = DataType.DECIMAL(10, 2);
        assertTrue(decimalType instanceof DataType.DecimalType);
        assertTrue(decimalType.isCompatible(new BigDecimal("123.45")));
        
        System.out.println("✓ 复杂数据类型测试通过");
    }
    
    @Test
    public void testTableTypes() {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), true)
        );
        
        Schema appendOnlySchema = new Schema(1, fields);
        assertEquals(TableType.APPEND_ONLY, TableType.fromSchema(appendOnlySchema));
        
        Schema pkSchema = new Schema(1, fields, Collections.singletonList("id"));
        assertEquals(TableType.PRIMARY_KEY, TableType.fromSchema(pkSchema));
        assertTrue(pkSchema.hasPrimaryKey());
        
        System.out.println("✓ 表类型测试通过");
    }
    
    @Test
    public void testMergeEngine() {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), true)
        );
        Schema schema = new Schema(1, fields, Collections.singletonList("id"));
        
        MergeEngine mergeEngine = new MergeEngine(schema, TableType.PRIMARY_KEY);
        
        List<Row> rows = Arrays.asList(
            new Row(new Object[]{1, "Alice"}),
            new Row(new Object[]{1, "Alice Updated"}),
            new Row(new Object[]{2, "Bob"})
        );
        
        List<Row> deduplicated = mergeEngine.deduplicate(rows);
        assertEquals(2, deduplicated.size());
        
        System.out.println("✓ 合并引擎测试通过");
    }
    
    @Test
    public void testDynamicPartitionGenerator() {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("dt", DataType.STRING(), false),
            new Field("data", DataType.STRING(), true)
        );
        Schema schema = new Schema(1, fields, Collections.emptyList(), 
                                   Collections.singletonList("dt"));
        
        DynamicPartitionGenerator generator = new DynamicPartitionGenerator(schema);
        Row row = new Row(new Object[]{1, "2024-01-01", "test data"});
        
        PartitionSpec spec = generator.generatePartitionSpec(row);
        assertEquals("2024-01-01", spec.get("dt"));
        
        LocalDateTime dateTime = LocalDateTime.of(2024, 1, 15, 10, 30);
        PartitionSpec timePartition = DynamicPartitionGenerator.generateTimePartition(dateTime);
        assertEquals("2024-01-15", timePartition.get("dt"));
        
        PartitionSpec hourlyPartition = DynamicPartitionGenerator.generateHourlyPartition(dateTime);
        assertEquals("2024-01-15", hourlyPartition.get("dt"));
        assertEquals("10", hourlyPartition.get("hour"));
        
        System.out.println("✓ 动态分区生成器测试通过");
    }
    
    @Test
    public void testPartitionStatistics() {
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("dt", "2024-01-01");
        PartitionSpec spec = new PartitionSpec(partitionValues);
        
        PartitionStatistics stats = PartitionStatistics.builder(spec)
            .rowCount(1000)
            .fileSize(10240)
            .fileCount(5)
            .build();
        
        assertEquals(1000, stats.getRowCount());
        assertEquals(10240, stats.getFileSize());
        assertEquals(5, stats.getFileCount());
        
        PartitionStatistics stats2 = PartitionStatistics.builder(spec)
            .rowCount(500)
            .fileSize(5120)
            .fileCount(3)
            .build();
        
        PartitionStatistics merged = stats.merge(stats2);
        assertEquals(1500, merged.getRowCount());
        assertEquals(15360, merged.getFileSize());
        assertEquals(8, merged.getFileCount());
        
        System.out.println("✓ 分区统计信息测试通过");
    }
    
    @Test
    public void testTableMetadataWithTableType() {
        TableMetadata metadata = new TableMetadata("test_table", "test_db", 1, TableType.PRIMARY_KEY);
        assertEquals(TableType.PRIMARY_KEY, metadata.getTableType());
        
        TableMetadata appendOnlyMetadata = TableMetadata.newBuilder("append_table", "test_db", 1)
            .tableType(TableType.APPEND_ONLY)
            .description("Append-only table")
            .build();
        
        assertEquals(TableType.APPEND_ONLY, appendOnlyMetadata.getTableType());
        assertEquals("Append-only table", appendOnlyMetadata.getDescription());
        
        System.out.println("✓ 表元数据类型测试通过");
    }
    
    @Test
    public void testRowWithComplexTypes() {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("tags", DataType.ARRAY(DataType.STRING()), true),
            new Field("attributes", DataType.MAP(DataType.STRING(), DataType.STRING()), true),
            new Field("created_at", DataType.TIMESTAMP(), true)
        );
        Schema schema = new Schema(1, fields);
        
        Map<String, String> attributes = new HashMap<>();
        attributes.put("color", "red");
        attributes.put("size", "large");
        
        Row row = new Row(new Object[]{
            1, 
            Arrays.asList("tag1", "tag2"), 
            attributes,
            LocalDateTime.now()
        });
        
        assertDoesNotThrow(() -> row.validate(schema));
        
        System.out.println("✓ 复杂类型数据行测试通过");
    }
}

