package org.sj4axao.stater.kudu.client;

import org.apache.kudu.client.*;

import java.util.List;

public interface KuduImpalaTemplate {
    long getId();

    List<String> getTablesList();

    KuduTable getTable(String paramString) throws KuduException;

    KuduTable getTable(String paramString1, String paramString2) throws KuduException;

    Insert createInsert(String paramString, Object paramObject) throws KuduException;

    Insert createInsert(String paramString1, String paramString2, Object paramObject) throws KuduException;

    Update createUpdate(String paramString, Object paramObject) throws KuduException;

    Update createUpdate(String paramString1, String paramString2, Object paramObject) throws KuduException;

    Delete createDelete(String paramString, Object paramObject) throws KuduException;

    Delete createDelete(String paramString1, String paramString2, Object paramObject) throws KuduException;

    Upsert createUpsert(String paramString, Object paramObject) throws KuduException;

    Upsert createUpsert(String paramString1, String paramString2, Object paramObject) throws KuduException;

    void apply(List<Operation> paramList) throws KuduException;

    void apply(Operation paramOperation) throws KuduException;

    void delete(String paramString, Object paramObject) throws KuduException;

    void delete(String paramString1, String paramString2, Object paramObject) throws KuduException;

    void insert(String paramString, Object paramObject) throws KuduException;

    void insert(String paramString1, String paramString2, Object paramObject) throws KuduException;

    void update(String paramString, Object paramObject) throws KuduException;

    void update(String paramString1, String paramString2, Object paramObject) throws KuduException;

    void upsert(String paramString, Object paramObject) throws KuduException;

    void upsert(String paramString1, String paramString2, Object paramObject) throws KuduException;

    String getDefaultDataBase();
}
