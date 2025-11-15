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
            return null;
        }
        
        Predicate result = null;
        for (ResolvedExpression filter : filters) {
            Predicate predicate = convertExpression(filter, schema);
            if (predicate != null) {
                result = result == null ? predicate : result.and(predicate);
            }
        }
        
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
                LOG.debug("Unsupported function: {}", functionName);
                return null;
        }
    }
    
    private static Predicate convertComparison(List<ResolvedExpression> children, 
                                               Predicate.CompareOp op, 
                                               ResolvedSchema schema) {
        if (children.size() != 2) {
            return null;
        }
        
        String fieldName = null;
        Object value = null;
        
        ResolvedExpression left = children.get(0);
        ResolvedExpression right = children.get(1);
        
        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            fieldName = ((FieldReferenceExpression) left).getName();
            value = extractLiteralValue((ValueLiteralExpression) right);
        } else if (right instanceof FieldReferenceExpression && left instanceof ValueLiteralExpression) {
            fieldName = ((FieldReferenceExpression) right).getName();
            value = extractLiteralValue((ValueLiteralExpression) left);
            op = reverseOp(op);
        } else {
            LOG.debug("Unsupported comparison operands");
            return null;
        }
        
        if (fieldName == null || value == null) {
            return null;
        }
        
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

