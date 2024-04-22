package org.sj4axao.stater.kudu.client;

import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.config.KuduProperties;

import java.util.List;

public interface KuduTemplate {
    KuduProperties getProperties();

    long getId();

    List<String> getTablesList();

    KuduTable getTable(String paramString) throws KuduException;

    List<String> getKeyColumns(String paramString) throws KuduException;

    Insert createInsert(String paramString, Object paramObject) throws KuduException;

    Update createUpdate(String paramString, Object paramObject) throws KuduException;

    Delete createDelete(String paramString, Object paramObject) throws KuduException;

    Upsert createUpsert(String paramString, Object paramObject) throws KuduException;

    void apply(List<Operation> paramList) throws KuduException;

    List<OperationResponse> apply(List<Operation> paramList, int paramInt) throws KuduException;

    void apply(Operation paramOperation) throws KuduException;

    void delete(String paramString, Object paramObject) throws KuduException;

    void insert(String paramString, Object paramObject) throws KuduException;

    void update(String paramString, Object paramObject) throws KuduException;

    void upsert(String paramString, Object paramObject) throws KuduException;
}
