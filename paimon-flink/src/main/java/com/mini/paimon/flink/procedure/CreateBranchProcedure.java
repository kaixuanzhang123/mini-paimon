package com.mini.paimon.flink.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Procedure：创建分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * - branch: 分支名称
 * - tag: Tag 名称（可选，null 表示创建空分支）
 * 
 * 用法示例（通过Java API）：
 * <pre>
 * CreateBranchProcedure proc = new CreateBranchProcedure(catalog);
 * String result = proc.call("db.table", "branch_name", null);
 * </pre>
 */
public class CreateBranchProcedure extends ProcedureBase {
    
    private static final Logger LOG = LoggerFactory.getLogger(CreateBranchProcedure.class);
    
    public CreateBranchProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "create_branch";
    }
    
    /**
     * 创建空分支
     * 
     * @param tableIdentifier 表标识符（格式：database.table）
     * @param branchName 分支名称
     * @return 操作结果消息
     */
    public String call(String tableIdentifier, String branchName) throws Exception {
        return call(tableIdentifier, branchName, null);
    }
    
    /**
     * 从 Tag 创建分支
     * 
     * @param tableIdentifier 表标识符（格式：database.table）
     * @param branchName 分支名称
     * @param tagName Tag 名称（null 表示创建空分支）
     * @return 操作结果消息
     */
    public String call(String tableIdentifier, String branchName, String tagName) throws Exception {
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
            // 调用 Catalog 创建分支
            catalog.createBranch(identifier, branchName, tagName);
            
            String message = tagName == null || tagName.trim().isEmpty()
                ? String.format("Created empty branch '%s' for table %s.%s", branchName, database, table)
                : String.format("Created branch '%s' from tag '%s' for table %s.%s", branchName, tagName, database, table);
            
            LOG.info(message);
            return message;
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to create branch '%s' for table %s.%s: %s", 
                branchName, database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

