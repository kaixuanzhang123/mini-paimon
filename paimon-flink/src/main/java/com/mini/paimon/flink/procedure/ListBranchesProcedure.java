package com.mini.paimon.flink.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Flink Procedure：列出所有分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * 
 * 用法示例（通过Java API）：
 * <pre>
 * ListBranchesProcedure proc = new ListBranchesProcedure(catalog);
 * List&lt;String&gt; branches = proc.call("db.table");
 * </pre>
 */
public class ListBranchesProcedure extends ProcedureBase {
    
    private static final Logger LOG = LoggerFactory.getLogger(ListBranchesProcedure.class);
    
    public ListBranchesProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "list_branches";
    }
    
    /**
     * 列出表的所有分支
     * 
     * @param tableIdentifier 表标识符（格式：database.table）
     * @return 分支列表
     */
    public List<String> call(String tableIdentifier) throws Exception {
        checkNotBlank(tableIdentifier, "table identifier");
        
        // 解析表标识符
        String[] parts = tableIdentifier.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid table identifier: " + tableIdentifier + ". Expected format: database.table");
        }
        
        String database = parts[0];
        String table = parts[1];
        Identifier identifier = new Identifier(database, table);
        
        try {
            // 调用 Catalog 列出分支
            List<String> branches = catalog.listBranches(identifier);
            
            LOG.info("Listed {} branches for table {}.{}", branches.size(), database, table);
            return branches;
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to list branches for table %s.%s: %s", 
                database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

