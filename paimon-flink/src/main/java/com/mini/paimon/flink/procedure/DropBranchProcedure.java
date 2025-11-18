package com.mini.paimon.flink.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Procedure：删除分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * - branch: 分支名称
 * 
 * 用法示例（通过Java API）：
 * <pre>
 * DropBranchProcedure proc = new DropBranchProcedure(catalog);
 * String result = proc.call("db.table", "branch_name");
 * </pre>
 */
public class DropBranchProcedure extends ProcedureBase {
    
    private static final Logger LOG = LoggerFactory.getLogger(DropBranchProcedure.class);
    
    public DropBranchProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "drop_branch";
    }
    
    /**
     * 删除分支
     * 
     * @param tableIdentifier 表标识符（格式：database.table）
     * @param branchName 分支名称
     * @return 操作结果消息
     */
    public String call(String tableIdentifier, String branchName) throws Exception {
        checkNotBlank(tableIdentifier, "table identifier");
        checkNotBlank(branchName, "branch name");
        
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
            // 调用 Catalog 删除分支
            catalog.dropBranch(identifier, branchName);
            
            String message = String.format("Dropped branch '%s' for table %s.%s", branchName, database, table);
            LOG.info(message);
            return message;
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to drop branch '%s' for table %s.%s: %s", 
                branchName, database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

