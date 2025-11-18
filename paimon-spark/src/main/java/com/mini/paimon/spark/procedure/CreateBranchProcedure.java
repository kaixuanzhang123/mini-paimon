package com.mini.paimon.spark.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark Procedure：创建分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * - branch: 分支名称
 * - tag: Tag 名称（可选，null 表示创建空分支）
 * 
 * 用法示例（通过Scala/Java API）：
 * <pre>
 * CreateBranchProcedure proc = new CreateBranchProcedure(catalog);
 * InternalRow args = InternalRow.apply(
 *     UTF8String.fromString("db.table"),
 *     UTF8String.fromString("branch_name"),
 *     UTF8String.fromString("tag_name") // 或 null
 * );
 * InternalRow[] result = proc.call(args);
 * </pre>
 */
public class CreateBranchProcedure extends BaseProcedure {
    
    private static final Logger LOG = LoggerFactory.getLogger(CreateBranchProcedure.class);
    
    public CreateBranchProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "create_branch";
    }
    
    @Override
    public StructType outputType() {
        return singleStringOutput();
    }
    
    /**
     * 执行创建分支操作
     * 
     * @param args 参数行，包含: (table, branch, tag)
     * @return 结果行数组
     */
    @Override
    public InternalRow[] call(InternalRow args) {
        // 提取参数
        UTF8String tableIdentifierUtf8 = args.getUTF8String(0);
        UTF8String branchNameUtf8 = args.getUTF8String(1);
        UTF8String tagNameUtf8 = args.numFields() > 2 ? args.getUTF8String(2) : null;
        
        String tableIdentifier = tableIdentifierUtf8 != null ? tableIdentifierUtf8.toString() : null;
        String branchName = branchNameUtf8 != null ? branchNameUtf8.toString() : null;
        String tagName = tagNameUtf8 != null && !tagNameUtf8.toString().isEmpty() 
            ? tagNameUtf8.toString() : null;
        
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
            
            String message = tagName == null
                ? String.format("Created empty branch '%s' for table %s.%s", branchName, database, table)
                : String.format("Created branch '%s' from tag '%s' for table %s.%s", branchName, tagName, database, table);
            
            LOG.info(message);
            return successRow(message);
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to create branch '%s' for table %s.%s: %s", 
                branchName, database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

