package com.mini.paimon.flink.catalog;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.index.IndexType;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Table;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkCatalog extends AbstractCatalog {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalog.class);
    
    private final Catalog catalog;
    private final String warehouse;
    private String defaultDatabase;

    public FlinkCatalog(String catalogName, Catalog catalog, String defaultDatabase, String warehouse) {
        super(catalogName, defaultDatabase);
        this.catalog = catalog;
        this.defaultDatabase = defaultDatabase;
        this.warehouse = warehouse;
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Opening Flink catalog: {}", getName());
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Closing Flink catalog: {}", getName());
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return catalog.listDatabases();
        } catch (Exception e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return new CatalogDatabaseImpl(new HashMap<>(), null);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return catalog.databaseExists(databaseName);
        } catch (Exception e) {
            throw new CatalogException("Failed to check database existence: " + databaseName, e);
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) 
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            if (databaseExists(name)) {
                if (!ignoreIfExists) {
                    throw new DatabaseAlreadyExistException(getName(), name);
                }
                return;
            }
            catalog.createDatabase(name, ignoreIfExists);
        } catch (DatabaseAlreadyExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to create database: " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) 
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            if (!databaseExists(name)) {
                if (!ignoreIfNotExists) {
                    throw new DatabaseNotExistException(getName(), name);
                }
                return;
            }
            
            if (!cascade && !listTables(name).isEmpty()) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            catalog.dropDatabase(name, ignoreIfNotExists, cascade);
        } catch (DatabaseNotExistException | DatabaseNotEmptyException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to drop database: " + name, e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) 
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterDatabase is not supported");
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            if (!databaseExists(databaseName)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
            return catalog.listTables(databaseName);
        } catch (DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to list tables in database: " + databaseName, e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        return java.util.Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {        try {
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            Table table = catalog.getTable(identifier);
            
            return FlinkTableFactory.createFlinkTable(table, warehouse);
        } catch (com.mini.paimon.exception.CatalogException e) {
            throw new TableNotExistException(getName(), tablePath);
        } catch (Exception e) {
            throw new CatalogException("Failed to get table: " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            return catalog.tableExists(identifier);
        } catch (Exception e) {
            throw new CatalogException("Failed to check table existence: " + tablePath, e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) 
            throws TableNotExistException, CatalogException {
        try {
            if (!tableExists(tablePath)) {
                if (!ignoreIfNotExists) {
                    throw new TableNotExistException(getName(), tablePath);
                }
                return;
            }
            
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            catalog.dropTable(identifier, ignoreIfNotExists);
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to drop table: " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) 
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("renameTable is not supported");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) 
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {
            if (!databaseExists(tablePath.getDatabaseName())) {
                throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
            }
            
            if (tableExists(tablePath)) {
                if (!ignoreIfExists) {
                    throw new TableAlreadyExistException(getName(), tablePath);
                }
                return;
            }
            
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            Schema schema = FlinkSchemaConverter.toSchema(table);
            
            Map<String, List<IndexType>> indexConfig = parseIndexConfig(table.getOptions());
            
            if (indexConfig.isEmpty()) {
                catalog.createTable(identifier, schema, ignoreIfExists);
            } else {
                LOG.info("Creating table {} with index config: {}", tablePath, indexConfig);
                catalog.createTableWithIndex(identifier, schema, indexConfig, ignoreIfExists);
            }
        } catch (TableAlreadyExistException | DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to create table: " + tablePath, e);
        }
    }
    
    private Map<String, List<IndexType>> parseIndexConfig(Map<String, String> options) {
        Map<String, List<IndexType>> indexConfig = new HashMap<>();
        
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("index.") && key.endsWith(".type")) {
                String fieldName = key.substring(6, key.length() - 5);
                String indexTypeName = entry.getValue();
                
                try {
                    IndexType indexType = IndexType.fromName(indexTypeName);
                    indexConfig.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(indexType);
                    LOG.debug("Parsed index config: field={}, type={}", fieldName, indexType);
                } catch (IllegalArgumentException e) {
                    LOG.warn("Unknown index type: {} for field: {}", indexTypeName, fieldName);
                }
            }
        }
        
        return indexConfig;
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) 
            throws TableNotExistException, CatalogException {
        try {
            if (!tableExists(tablePath)) {
                if (!ignoreIfNotExists) {
                    throw new TableNotExistException(getName(), tablePath);
                }
                return;
            }
            
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            Schema newSchema = FlinkSchemaConverter.toSchema(newTable);
            
            catalog.alterTable(identifier, newSchema.getFields(), 
                              newSchema.getPrimaryKeys(), newSchema.getPartitionKeys());
            
            LOG.info("Altered table: {}", tablePath);
        } catch (TableNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to alter table: " + tablePath, e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) 
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        try {
            Identifier identifier = new Identifier(tablePath.getDatabaseName(), tablePath.getObjectName());
            Table table = catalog.getTable(identifier);
            Schema schema = catalog.getTableSchema(identifier);
            
            if (schema.getPartitionKeys().isEmpty()) {
                throw new TableNotPartitionedException(getName(), tablePath);
            }
            
            List<com.mini.paimon.partition.PartitionSpec> partitionSpecs = catalog.listPartitions(
                tablePath.getDatabaseName(), 
                tablePath.getObjectName()
            );
            
            List<CatalogPartitionSpec> catalogPartitionSpecs = new java.util.ArrayList<>();
            for (com.mini.paimon.partition.PartitionSpec spec : partitionSpecs) {
                java.util.LinkedHashMap<String, String> partitionMap = new java.util.LinkedHashMap<>(spec.getPartitionValues());
                catalogPartitionSpecs.add(new CatalogPartitionSpec(partitionMap));
            }
            return catalogPartitionSpecs;
        } catch (com.mini.paimon.exception.CatalogException.TableNotExistException e) {
            throw new TableNotExistException(getName(), tablePath);
        } catch (Exception e) {
            throw new CatalogException("Failed to list partitions", e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) 
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        throw new UnsupportedOperationException("listPartitions is not supported");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) 
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException("listPartitionsByFilter is not supported");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) 
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("getPartition is not supported");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) 
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, 
                                CatalogPartition partition, boolean ignoreIfExists) 
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, 
                   PartitionAlreadyExistsException, CatalogException {
        throw new UnsupportedOperationException("createPartition is not supported");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) 
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("dropPartition is not supported");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, 
                               CatalogPartition newPartition, boolean ignoreIfNotExists) 
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartition is not supported");
    }

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return java.util.Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) 
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) 
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("createFunction is not supported");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) 
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterFunction is not supported");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) 
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException("dropFunction is not supported");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) 
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) 
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) 
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) 
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) 
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterTableStatistics is not supported");
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) 
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterTableColumnStatistics is not supported");
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, 
                                        CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) 
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartitionStatistics is not supported");
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, 
                                              CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) 
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException("alterPartitionColumnStatistics is not supported");
    }
}
