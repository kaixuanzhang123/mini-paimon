package com.mini.paimon.branch;

import java.util.List;

/**
 * 分支管理器接口
 * 参考 Apache Paimon 的 BranchManager 设计
 * 
 * 负责分支的生命周期管理：创建、删除、列举
 */
public interface BranchManager {
    
    /** 分支名前缀 */
    String BRANCH_PREFIX = "branch-";
    
    /** 默认主分支名 */
    String DEFAULT_MAIN_BRANCH = "main";
    
    /**
     * 创建空分支
     * 
     * @param branchName 分支名
     * @throws IllegalArgumentException 如果分支名非法
     * @throws RuntimeException 如果创建失败
     */
    void createBranch(String branchName);
    
    /**
     * 从指定 Tag 创建分支
     * 
     * @param branchName 分支名
     * @param tagName Tag 名称（可为 null，表示创建空分支）
     * @throws IllegalArgumentException 如果分支名非法
     * @throws RuntimeException 如果创建失败
     */
    void createBranch(String branchName, String tagName);
    
    /**
     * 删除分支
     * 
     * @param branchName 分支名
     * @throws IllegalArgumentException 如果分支不存在
     * @throws RuntimeException 如果删除失败
     */
    void dropBranch(String branchName);
    
    /**
     * 列出所有分支
     * 
     * @return 分支名列表（不包括主分支 main）
     */
    List<String> branches();
    
    /**
     * 检查分支是否存在
     * 
     * @param branchName 分支名
     * @return true 如果分支存在
     */
    default boolean branchExists(String branchName) {
        return branches().contains(branchName);
    }
    
    // ==================== 静态工具方法 ====================
    
    /**
     * 规范化分支名：空字符串或 null 转换为 main
     * 
     * @param branch 分支名
     * @return 规范化后的分支名
     */
    static String normalizeBranch(String branch) {
        if (branch == null || branch.trim().isEmpty()) {
            return DEFAULT_MAIN_BRANCH;
        }
        return branch.trim();
    }
    
    /**
     * 判断是否为主分支
     * 
     * @param branch 分支名
     * @return true 如果是主分支
     */
    static boolean isMainBranch(String branch) {
        return DEFAULT_MAIN_BRANCH.equals(normalizeBranch(branch));
    }
    
    /**
     * 验证分支名的合法性
     * 
     * @param branchName 分支名
     * @throws IllegalArgumentException 如果分支名非法
     */
    static void validateBranch(String branchName) {
        if (branchName == null) {
            throw new IllegalArgumentException("Branch name cannot be null");
        }
        
        String normalized = branchName.trim();
        
        // 不能使用 main 作为自定义分支名
        if (DEFAULT_MAIN_BRANCH.equals(normalized)) {
            throw new IllegalArgumentException(
                "Branch name '" + DEFAULT_MAIN_BRANCH + "' is the default branch and cannot be used.");
        }
        
        // 不能为空白字符串
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Branch name cannot be blank.");
        }
        
        // 不能是纯数字字符串（避免与 snapshot ID 混淆）
        if (normalized.matches("\\d+")) {
            throw new IllegalArgumentException(
                "Branch name cannot be pure numeric string but is '" + normalized + "'.");
        }
        
        // 只能包含字母、数字、下划线和连字符
        if (!normalized.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException(
                "Branch name can only contain letters, numbers, underscores and hyphens: " + normalized);
        }
    }
    
    /**
     * 计算分支的路径
     * 主分支使用表根目录，自定义分支使用 branch/branch-{name} 子目录
     * 
     * @param tablePath 表根路径
     * @param branch 分支名
     * @return 分支路径
     */
    static String branchPath(String tablePath, String branch) {
        if (isMainBranch(branch)) {
            return tablePath;
        }
        return tablePath + "/branch/" + BRANCH_PREFIX + branch;
    }
}

