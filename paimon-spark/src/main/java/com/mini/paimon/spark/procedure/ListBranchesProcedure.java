package com.mini.paimon.spark.procedure;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Spark Procedure：列出所有分支
 * 
 * 参数：
 * - table: 表标识符（格式：database.table）
 * 
 * 用法示例（通过Scala/Java API）：
 * <pre>
 * ListBranchesProcedure proc = new ListBranchesProcedure(catalog);
 * InternalRow args = InternalRow.apply(
 *     UTF8String.fromString("db.table")
 * );
 * InternalRow[] result = proc.call(args);
 * </pre>
 */
public class ListBranchesProcedure extends BaseProcedure {
    
    private static final Logger LOG = LoggerFactory.getLogger(ListBranchesProcedure.class);
    
    public ListBranchesProcedure(Catalog catalog) {
        super(catalog);
    }
    
    @Override
    public String name() {
        return "list_branches";
    }
    
    @Override
    public StructType outputType() {
        return branchListOutput();
    }
    
    /**
     * 执行列出分支操作
     * 
     * @param args 参数行，包含: (table)
     * @return 结果行数组，每个分支一行
     */
    @Override
    public InternalRow[] call(InternalRow args) {
        // 提取参数
        UTF8String tableIdentifierUtf8 = args.getUTF8String(0);
        String tableIdentifier = tableIdentifierUtf8 != null ? tableIdentifierUtf8.toString() : null;
        
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
            
            // 转换为 InternalRow 数组
            InternalRow[] result = new InternalRow[branches.size()];
            for (int i = 0; i < branches.size(); i++) {
                result[i] = new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(
                    new Object[]{UTF8String.fromString(branches.get(i))}
                );
            }
            
            LOG.info("Listed {} branches for table {}.{}", branches.size(), database, table);
            return result;
            
        } catch (Exception e) {
            String errorMsg = String.format("Failed to list branches for table %s.%s: %s", 
                database, table, e.getMessage());
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);
        }
    }
}

