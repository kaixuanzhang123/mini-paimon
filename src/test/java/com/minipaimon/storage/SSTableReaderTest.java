package com.minipaimon.storage;

import com.minipaimon.metadata.Row;
import com.minipaimon.utils.PathFactory;

import java.io.IOException;
import java.util.List;

public class SSTableReaderTest {
    public static void main(String[] args) {
        try {
            PathFactory pathFactory = new PathFactory("./warehouse");
            String sstPath = pathFactory.getSSTPath("default", "users", 0, 0).toString();
            
            System.out.println("Reading SSTable: " + sstPath);
            
            SSTableReader reader = new SSTableReader();
            List<Row> rows = reader.scan(sstPath);
            
            System.out.println("Found " + rows.size() + " rows:");
            for (int i = 0; i < rows.size(); i++) {
                System.out.println("Row " + (i + 1) + ": " + rows.get(i));
            }
        } catch (IOException e) {
            System.err.println("Error reading SSTable: " + e.getMessage());
            e.printStackTrace();
        }
    }
}