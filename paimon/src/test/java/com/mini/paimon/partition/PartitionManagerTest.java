package com.mini.paimon.partition;

import com.mini.paimon.utils.PathFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分区管理器测试
 */
public class PartitionManagerTest {
    
    private Path tempDir;
    private PathFactory pathFactory;
    private PartitionManager partitionManager;
    
    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("partition-test");
        pathFactory = new PathFactory(tempDir.toString());
        
        List<String> partitionKeys = Arrays.asList("dt", "hour");
        partitionManager = new PartitionManager(pathFactory, "default", "test_table", partitionKeys);
    }
    
    @AfterEach
    public void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
    
    @Test
    public void testCreatePartition() throws IOException {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", "2024-01-01");
        partitionValues.put("hour", "10");
        PartitionSpec spec = new PartitionSpec(partitionValues);
        
        partitionManager.createPartition(spec);
        
        assertTrue(partitionManager.partitionExists(spec), "Partition should exist");
    }
    
    @Test
    public void testDropPartition() throws IOException {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("dt", "2024-01-01");
        partitionValues.put("hour", "10");
        PartitionSpec spec = new PartitionSpec(partitionValues);
        
        partitionManager.createPartition(spec);
        assertTrue(partitionManager.partitionExists(spec), "Partition should exist");
        
        partitionManager.dropPartition(spec);
        assertFalse(partitionManager.partitionExists(spec), "Partition should not exist");
    }
    
    @Test
    public void testPartitionSpec() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "2024-01-01");
        values.put("hour", "10");
        
        PartitionSpec spec = PartitionSpec.of(values);
        
        assertEquals("dt=2024-01-01/hour=10", spec.toPath());
        assertEquals("2024-01-01", spec.get("dt"));
        assertEquals("10", spec.get("hour"));
    }
    
    @Test
    public void testPartitionPath() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("dt", "2024-01-01");
        values.put("hour", "10");
        
        PartitionSpec spec = PartitionSpec.of(values);
        Path partitionPath = partitionManager.getPartitionPath(spec);
        
        assertTrue(partitionPath.toString().contains("dt=2024-01-01"),
            "Path should contain partition values");
        assertTrue(partitionPath.toString().contains("hour=10"),
            "Path should contain partition values");
    }
}
