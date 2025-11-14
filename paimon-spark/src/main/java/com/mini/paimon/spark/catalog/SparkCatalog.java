package com.mini.paimon.spark.catalog;

import com.mini.paimon.catalog.Catalog;
import com.mini.paimon.catalog.Identifier;
import com.mini.paimon.exception.CatalogException;
import com.mini.paimon.metadata.Schema;
import com.mini.paimon.table.Table;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SparkCatalog implements TableCatalog, SupportsNamespaces {

    private static final Logger LOG = LoggerFactory.getLogger(SparkCatalog.class);

    private String catalogName;
    private Catalog catalog;
    private String warehouse;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.warehouse = options.get("warehouse");
        
        if (warehouse == null) {
            throw new IllegalArgumentException("Warehouse path must be specified");
        }
        
        try {
            // 使用 CatalogLoader 通过 SPI 机制加载 Catalog
            com.mini.paimon.catalog.CatalogContext context = 
                com.mini.paimon.catalog.CatalogContext.builder()
                    .warehouse(warehouse)
                    .option("catalog.name", name)
                    .option("catalog.default-database", "default")
                    .build();
            this.catalog = com.mini.paimon.catalog.CatalogLoader.load("filesystem", context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Paimon catalog", e);
        }
        
        LOG.info("Initialized Spark catalog: {} with warehouse: {}", name, warehouse);
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(org.apache.spark.sql.connector.catalog.Identifier ident) 
            throws NoSuchTableException {
        try {
            Identifier identifier = new Identifier(
                ident.namespace()[0], 
                ident.name()
            );
            
            Table paimonTable = catalog.getTable(identifier);
            return new SparkTable(paimonTable, warehouse);
        } catch (CatalogException e) {
            throw new NoSuchTableException(ident);
        }
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table createTable(
            org.apache.spark.sql.connector.catalog.Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
        
        try {
            if (ident.namespace().length == 0) {
                throw new NoSuchNamespaceException(new String[]{});
            }
            
            String database = ident.namespace()[0];
            if (!catalog.databaseExists(database)) {
                throw new NoSuchNamespaceException(ident.namespace());
            }
            
            Identifier identifier = new Identifier(database, ident.name());
            Schema paimonSchema = SparkSchemaConverter.toSchema(schema, partitions, properties);
            
            catalog.createTable(identifier, paimonSchema, false);
            
            Table paimonTable = catalog.getTable(identifier);
            return new SparkTable(paimonTable, warehouse);
        } catch (CatalogException.TableAlreadyExistException e) {
            throw new TableAlreadyExistsException(ident);
        } catch (CatalogException.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(ident.namespace());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create table: " + ident, e);
        }
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table alterTable(
            org.apache.spark.sql.connector.catalog.Identifier ident,
            TableChange... changes) throws NoSuchTableException {
        throw new UnsupportedOperationException("alterTable is not supported");
    }

    @Override
    public boolean dropTable(org.apache.spark.sql.connector.catalog.Identifier ident) {
        try {
            Identifier identifier = new Identifier(
                ident.namespace()[0], 
                ident.name()
            );
            catalog.dropTable(identifier, true);
            return true;
        } catch (Exception e) {
            LOG.error("Failed to drop table: {}", ident, e);
            return false;
        }
    }

    @Override
    public void renameTable(
            org.apache.spark.sql.connector.catalog.Identifier oldIdent,
            org.apache.spark.sql.connector.catalog.Identifier newIdent) 
            throws NoSuchTableException, TableAlreadyExistsException {
        throw new UnsupportedOperationException("renameTable is not supported");
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Identifier[] listTables(String[] namespace) 
            throws NoSuchNamespaceException {
        try {
            if (namespace.length == 0) {
                throw new NoSuchNamespaceException(namespace);
            }
            
            String database = namespace[0];
            List<String> tables = catalog.listTables(database);
            
            return tables.stream()
                .map(table -> org.apache.spark.sql.connector.catalog.Identifier.of(namespace, table))
                .toArray(org.apache.spark.sql.connector.catalog.Identifier[]::new);
        } catch (CatalogException.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        } catch (Exception e) {
            throw new RuntimeException("Failed to list tables in namespace: " + Arrays.toString(namespace), e);
        }
    }

    @Override
    public String[][] listNamespaces() {
        try {
            List<String> databases = catalog.listDatabases();
            return databases.stream()
                .map(db -> new String[]{db})
                .toArray(String[][]::new);
        } catch (Exception e) {
            LOG.error("Failed to list namespaces", e);
            return new String[0][];
        }
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) 
            throws NoSuchNamespaceException {
        try {
            if (namespace.length == 0) {
                throw new NoSuchNamespaceException(namespace);
            }
            
            String database = namespace[0];
            if (!catalog.databaseExists(database)) {
                throw new NoSuchNamespaceException(namespace);
            }
            
            return new HashMap<>();
        } catch (Exception e) {
            throw new NoSuchNamespaceException(namespace);
        }
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) 
            throws NamespaceAlreadyExistsException {
        try {
            if (namespace.length == 0) {
                throw new IllegalArgumentException("Namespace cannot be empty");
            }
            
            String database = namespace[0];
            catalog.createDatabase(database, false);
        } catch (CatalogException.DatabaseAlreadyExistException e) {
            throw new NamespaceAlreadyExistsException(namespace);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create namespace: " + Arrays.toString(namespace), e);
        }
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) 
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("alterNamespace is not supported");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException {
        try {
            if (namespace.length == 0) {
                throw new NoSuchNamespaceException(namespace);
            }
            
            String database = namespace[0];
            catalog.dropDatabase(database, true, cascade);
            return true;
        } catch (CatalogException.DatabaseNotExistException e) {
            throw new NoSuchNamespaceException(namespace);
        } catch (Exception e) {
            LOG.error("Failed to drop namespace: {}", Arrays.toString(namespace), e);
            return false;
        }
    }
}

