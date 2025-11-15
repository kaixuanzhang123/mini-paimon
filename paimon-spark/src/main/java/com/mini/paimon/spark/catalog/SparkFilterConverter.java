package com.mini.paimon.spark.catalog;

import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Predicate;
import org.apache.spark.sql.sources.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkFilterConverter {
    
    private static final Logger LOG = LoggerFactory.getLogger(SparkFilterConverter.class);
    
    public static Predicate convert(Filter[] filters, Schema schema) {
        if (filters == null || filters.length == 0) {
            return null;
        }
        
        Predicate result = null;
        for (Filter filter : filters) {
            Predicate predicate = convertFilter(filter, schema);
            if (predicate != null) {
                result = result == null ? predicate : result.and(predicate);
            }
        }
        
        return result;
    }
    
    private static Predicate convertFilter(Filter filter, Schema schema) {
        if (filter instanceof And) {
            And and = (And) filter;
            Predicate left = convertFilter(and.left(), schema);
            Predicate right = convertFilter(and.right(), schema);
            if (left != null && right != null) {
                return left.and(right);
            }
            return left != null ? left : right;
        }
        
        if (filter instanceof Or) {
            Or or = (Or) filter;
            Predicate left = convertFilter(or.left(), schema);
            Predicate right = convertFilter(or.right(), schema);
            if (left != null && right != null) {
                return left.or(right);
            }
            return null;
        }
        
        if (filter instanceof Not) {
            LOG.debug("NOT filter not supported yet");
            return null;
        }
        
        if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            return Predicate.equal(eq.attribute(), eq.value());
        }
        
        if (filter instanceof EqualNullSafe) {
            EqualNullSafe eq = (EqualNullSafe) filter;
            return Predicate.equal(eq.attribute(), eq.value());
        }
        
        if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            return Predicate.greaterThan(gt.attribute(), gt.value());
        }
        
        if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual ge = (GreaterThanOrEqual) filter;
            return Predicate.greaterOrEqual(ge.attribute(), ge.value());
        }
        
        if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            return Predicate.lessThan(lt.attribute(), lt.value());
        }
        
        if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual le = (LessThanOrEqual) filter;
            return Predicate.lessOrEqual(le.attribute(), le.value());
        }
        
        LOG.debug("Unsupported filter type: {}", filter.getClass().getName());
        return null;
    }
}

