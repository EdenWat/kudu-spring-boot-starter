package org.sj4axao.stater.kudu.client.impl;

import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.client.KuduImpalaTemplate;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.config.KuduProperties;
import org.sj4axao.stater.kudu.exception.DefaultDBNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class PlainKuduImpalaTemplate implements KuduImpalaTemplate {
    private static final Logger log = LoggerFactory.getLogger(PlainKuduImpalaTemplate.class);

    private static final String TABLE_PREFIX = "impala::";

    private static final String DOT = ".";

    private String defaultDataBase;

    private KuduProperties kuduProperties;

    private KuduTemplate kuduTemplate;

    public PlainKuduImpalaTemplate(KuduTemplate kuduTemplate) {
        this.kuduTemplate = kuduTemplate;
        this.kuduProperties = kuduTemplate.getProperties();
        this.defaultDataBase = Optional.<String>ofNullable(this.kuduProperties.getDefaultDataBase()).orElseGet(() -> {
            log.warn("kudu dbName");
            return null;
        });
    }

    public long getId() {
        return this.kuduTemplate.getId();
    }

    public List<String> getTablesList() {
        return this.kuduTemplate.getTablesList();
    }

    public KuduTable getTable(String tableName) throws KuduException {
        return getTable(getDefaultDataBase(), tableName);
    }

    public KuduTable getTable(String dbName, String tableName) throws KuduException {
        String finalTableName = getFinalTableName(dbName, tableName);
        return this.kuduTemplate.getTable(finalTableName);
    }

    public Insert createInsert(String table, Object data) throws KuduException {
        return createInsert(getDefaultDataBase(), table, data);
    }

    public Insert createInsert(String dbName, String table, Object data) throws KuduException {
        return this.kuduTemplate.createInsert(getFinalTableName(dbName, table), data);
    }

    public Update createUpdate(String table, Object data) throws KuduException {
        return createUpdate(getDefaultDataBase(), table, data);
    }

    public Update createUpdate(String dbName, String table, Object data) throws KuduException {
        return this.kuduTemplate.createUpdate(getFinalTableName(dbName, table), data);
    }

    public Delete createDelete(String table, Object data) throws KuduException {
        return createDelete(getDefaultDataBase(), table, data);
    }

    public Delete createDelete(String dbName, String table, Object data) throws KuduException {
        return this.kuduTemplate.createDelete(getFinalTableName(dbName, table), data);
    }

    public Upsert createUpsert(String table, Object data) throws KuduException {
        return createUpsert(getDefaultDataBase(), table, data);
    }

    public Upsert createUpsert(String dbName, String table, Object data) throws KuduException {
        return this.kuduTemplate.createUpsert(getFinalTableName(dbName, table), data);
    }

    public void apply(List<Operation> operations) throws KuduException {
        this.kuduTemplate.apply(operations);
    }

    public void apply(Operation operation) throws KuduException {
        this.kuduTemplate.apply(operation);
    }

    public void delete(String table, Object data) throws KuduException {
        delete(getDefaultDataBase(), table, data);
    }

    public void delete(String dbName, String table, Object data) throws KuduException {
        this.kuduTemplate.delete(getFinalTableName(dbName, table), data);
    }

    public void insert(String table, Object data) throws KuduException {
        insert(getDefaultDataBase(), table, data);
    }

    public void insert(String dbName, String table, Object data) throws KuduException {
        this.kuduTemplate.insert(getFinalTableName(dbName, table), data);
    }

    public void update(String table, Object data) throws KuduException {
        update(getDefaultDataBase(), table, data);
    }

    public void update(String dbName, String table, Object data) throws KuduException {
        this.kuduTemplate.update(getFinalTableName(dbName, table), data);
    }

    public void upsert(String table, Object data) throws KuduException {
        upsert(getDefaultDataBase(), table, data);
    }

    public void upsert(String dbName, String table, Object data) throws KuduException {
        this.kuduTemplate.upsert(getFinalTableName(dbName, table), data);
    }

    public String getDefaultDataBase() {
        return (String) Optional.<String>ofNullable(this.defaultDataBase).orElseThrow(() -> new DefaultDBNotFoundException("kudu.default-data-base"));
    }

    private String getFinalTableName(String dbName, String tableName) {
        return "impala::" + dbName + "." + tableName;
    }
}
