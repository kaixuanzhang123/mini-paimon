package com.mini.paimon.storage;

import com.mini.paimon.metadata.DataType;
import com.mini.paimon.metadata.Field;
import com.mini.paimon.metadata.Row;
import com.mini.paimon.metadata.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CsvWriterTest {
    
    private static final String TEST_DIR = "./test_csv_writer";
    private Path testPath;
    
    @BeforeEach
    public void setup() throws IOException {
        testPath = Paths.get(TEST_DIR);
        Files.createDirectories(testPath);
    }
    
    @AfterEach
    public void cleanup() throws IOException {
        if (Files.exists(testPath)) {
            Files.walk(testPath)
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
    public void testBasicCsvWrite() throws IOException {
        List<Field> fields = Arrays.asList(
            new Field("id", DataType.INT(), false),
            new Field("name", DataType.STRING(), true),
            new Field("age", DataType.INT(), true)
        );
        Schema schema = new Schema(0, fields);
        
        Path csvFile = testPath.resolve("test.csv");
        CsvWriter writer = new CsvWriter(schema, csvFile);
        
        writer.write(new Row(new Object[]{1, "Alice", 25}));
        writer.write(new Row(new Object[]{2, "Bob", 30}));
        writer.write(new Row(new Object[]{3, "Charlie", 35}));
        
        writer.close();
        
        assertTrue(Files.exists(csvFile));
        assertEquals(3, writer.getRowCount());
        
        List<String> lines = Files.readAllLines(csvFile);
        assertEquals(4, lines.size());
        assertEquals("id,name,age", lines.get(0));
        assertEquals("1,Alice,25", lines.get(1));
        
        System.out.println("✓ CSV写入测试通过");
    }
}
