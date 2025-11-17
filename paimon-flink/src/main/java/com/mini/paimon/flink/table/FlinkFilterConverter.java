package com.mini.paimon.flink.table;

import com.mini.paimon.table.Predicate;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FlinkFilterConverter {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkFilterConverter.class);
    
    public static Predicate convert(List<ResolvedExpression> filters, ResolvedSchema schema) {
        if (filters == null || filters.isEmpty()) {
            LOG.info("No filters to convert");
            return null;
        }
        
        LOG.info("Converting {} filter expressions to Paimon predicates", filters.size());
        Predicate result = null;
        for (ResolvedExpression filter : filters) {
            LOG.info("Processing filter expression: {}", filter);
            Predicate predicate = convertExpression(filter, schema);
            if (predicate != null) {
                LOG.info("Converted to predicate: {}", predicate);
                result = result == null ? predicate : result.and(predicate);
            } else {
                LOG.warn("Failed to convert filter expression: {}", filter);
            }
        }
        
        LOG.info("Final combined predicate: {}", result);
        return result;
    }
    
    private static Predicate convertExpression(ResolvedExpression expression, ResolvedSchema schema) {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            return convertCallExpression(call, schema);
        }
        
        LOG.debug("Unsupported filter expression: {}", expression);
        return null;
    }
    
    private static Predicate convertCallExpression(CallExpression call, ResolvedSchema schema) {
        String functionName = call.getFunctionName();
        List<ResolvedExpression> children = call.getResolvedChildren();
        
        LOG.info("Converting call expression: function={}, children={}", functionName, children.size());
        
        switch (functionName) {
            case "equals":
            case "=":
                return convertComparison(children, Predicate.CompareOp.EQ, schema);
            case "notEquals":
            case "<>":
                return convertComparison(children, Predicate.CompareOp.NE, schema);
            case "greaterThan":
            case ">":
                return convertComparison(children, Predicate.CompareOp.GT, schema);
            case "greaterThanOrEqual":
            case ">=":
                return convertComparison(children, Predicate.CompareOp.GE, schema);
            case "lessThan":
            case "<":
                return convertComparison(children, Predicate.CompareOp.LT, schema);
            case "lessThanOrEqual":
            case "<=":
                return convertComparison(children, Predicate.CompareOp.LE, schema);
            case "and":
            case "AND":
                return convertAnd(children, schema);
            case "or":
            case "OR":
                return convertOr(children, schema);
            default:
                LOG.warn("Unsupported function: {}", functionName);
                return null;
        }
    }
    
    private static Predicate convertComparison(List<ResolvedExpression> children, 
                                               Predicate.CompareOp op, 
                                               ResolvedSchema schema) {
        if (children.size() != 2) {
            LOG.warn("Comparison requires exactly 2 operands, got {}", children.size());
            return null;
        }
        
        String fieldName = null;
        Object value = null;
        
        ResolvedExpression left = children.get(0);
        ResolvedExpression right = children.get(1);
        
        LOG.info("Comparison operands: left={}, right={}", left.getClass().getSimpleName(), right.getClass().getSimpleName());
        
        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            fieldName = ((FieldReferenceExpression) left).getName();
            value = extractLiteralValue((ValueLiteralExpression) right);
            LOG.info("Extracted: field={}, value={}, op={}", fieldName, value, op);
        } else if (right instanceof FieldReferenceExpression && left instanceof ValueLiteralExpression) {
            fieldName = ((FieldReferenceExpression) right).getName();
            value = extractLiteralValue((ValueLiteralExpression) left);
            op = reverseOp(op);
            LOG.info("Extracted (reversed): field={}, value={}, op={}", fieldName, value, op);
        } else {
            LOG.warn("Unsupported comparison operands: left={}, right={}", 
                left.getClass().getName(), right.getClass().getName());
            return null;
        }
        
        if (fieldName == null) {
            LOG.warn("Field name is null");
            return null;
        }
        if (value == null) {
            LOG.warn("Value is null for field {}", fieldName);
            return null;
        }
        
        LOG.info("Created FieldPredicate: {} {} {}", fieldName, op, value);
        return new Predicate.FieldPredicate(fieldName, op, value);
    }
    
    private static Predicate convertAnd(List<ResolvedExpression> children, ResolvedSchema schema) {
        if (children.size() != 2) {
            return null;
        }
        
        Predicate left = convertExpression(children.get(0), schema);
        Predicate right = convertExpression(children.get(1), schema);
        
        if (left == null || right == null) {
            return left != null ? left : right;
        }
        
        return left.and(right);
    }
    
    private static Predicate convertOr(List<ResolvedExpression> children, ResolvedSchema schema) {
        if (children.size() != 2) {
            return null;
        }
        
        Predicate left = convertExpression(children.get(0), schema);
        Predicate right = convertExpression(children.get(1), schema);
        
        if (left == null || right == null) {
            return null;
        }
        
        return left.or(right);
    }
    
    private static Object extractLiteralValue(ValueLiteralExpression literal) {
        return literal.getValueAs(literal.getOutputDataType().getConversionClass())
                .orElse(null);
    }
    
    private static Predicate.CompareOp reverseOp(Predicate.CompareOp op) {
        switch (op) {
            case GT: return Predicate.CompareOp.LT;
            case GE: return Predicate.CompareOp.LE;
            case LT: return Predicate.CompareOp.GT;
            case LE: return Predicate.CompareOp.GE;
            default: return op;
        }
    }
}

