package com.mini.paimon.sql;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.utils.PathFactory;

import java.io.IOException;

/**
 * SQL解析器别名，指向新的ANTLR4实现
 * 保持向后兼容
 */
public class SQLParser {
    private final SQLParserV2 delegate;
    
    public SQLParser(Catalog catalog, PathFactory pathFactory) {
        this.delegate = new SQLParserV2(catalog, pathFactory);
    }
    
    public void executeSQL(String sql) throws IOException {
        delegate.executeSQL(sql);
    }
}
