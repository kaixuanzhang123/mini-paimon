package com.mini.paimon.spark.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark Procedure：删除分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * - branch: 分支名称
 * 
 * 用法示例（通过Scala/Java API）：
 * <pre>
 * DropBranchProcedure proc = new DropBranchProcedure(catalog);
 * InternalRow args = InternalRow.apply(
 *     UTF8String.fromString("db.table"),
 *     UTF8String.fromString("branch_name")
 * );
 * InternalRow[] result = proc.call(args);
 * </pre>
 */
public class DropBranchProcedure extends BaseProcedure {
    
    private static final Logger LOG = LoggerFactory.getLogger(DropBranchProcedure.class);
    
    public DropBranchProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "drop_branch";
    }
    
    @Override
    public StructType outputType() {
        return singleStringOutput();
    }
    
    /**
     * 执行删除分支操作
     * 
     * @param args 参数行，包含: (table, branch)
     * @return 结果行数组
     */
    @Override
    public InternalRow[] call(InternalRow args) {
        // 提取参数
        UTF8String tableIdentifierUtf8 = args.getUTF8String(0);
        UTF8String branchNameUtf8 = args.getUTF8String(1);
        
        String tableIdentifier = tableIdentifierUtf8 != null ? tableIdentifierUtf8.toString() : null;
        String branchName = branchNameUtf8 != null ? branchNameUtf8.toString() : null;
        
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
            return successRow(message);
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to drop branch '%s' for table %s.%s: %s", 
                branchName, database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

