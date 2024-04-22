package org.sj4axao.stater.kudu.client.impl;

import com.alibaba.fastjson.util.TypeUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.sj4axao.stater.kudu.client.KuduTemplate;
import org.sj4axao.stater.kudu.config.KuduProperties;
import org.sj4axao.stater.kudu.enums.OperationType;
import org.sj4axao.stater.kudu.utils.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PlainKuduTemplate implements KuduTemplate {
    private static final Logger log = LoggerFactory.getLogger(PlainKuduTemplate.class);

    private KuduSession kuduSession;

    private KuduClient kuduClient;

    KuduProperties kuduProperties;

    private IdGenerator idGenerator;

    private static Map<String, KuduTable> tables = new HashMap<>();

    private static Map<String, List<String>> tablesKeys = new HashMap<>();

    private int flushSize = 16;

    public PlainKuduTemplate(KuduClient kuduClient, KuduSession kuduSession, KuduProperties kuduProperties) {
        this.kuduClient = kuduClient;
        this.kuduSession = kuduSession;
        this.kuduProperties = kuduProperties;
        if (kuduProperties.getBatchSize() > 0)
            this.flushSize = kuduProperties.getBatchSize();
        Long wordId = kuduProperties.getWorkerId();
        this.idGenerator = new IdGenerator(wordId.longValue());
    }

    public KuduProperties getProperties() {
        return this.kuduProperties;
    }

    public long getId() {
        return this.idGenerator.nextId();
    }

    public List<String> getTablesList() {
        try {
            return this.kuduClient.getTablesList().getTablesList();
        } catch (KuduException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void apply(List<Operation> operations) throws KuduException {
        long start = System.currentTimeMillis();
        int index = 0;
        for (Operation operation : operations) {
            try {
                this.kuduSession.apply(operation);
                if (++index % this.flushSize == 0)
                    printResposse(this.kuduSession.flush());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        printResposse(this.kuduSession.flush());
        log.info("kudu {}{} ms", Integer.valueOf(operations.size()), Long.valueOf(System.currentTimeMillis() - start));
    }

    public List<OperationResponse> apply(List<Operation> operations, int flushSize) throws KuduException {
        long start = System.currentTimeMillis();
        int index = 0;
        List<OperationResponse> operationResponseList = new ArrayList<>();
        for (Operation operation : operations) {
            try {
                this.kuduSession.apply(operation);
                if (++index % flushSize == 0) {
                    operationResponseList.addAll(this.kuduSession.flush());
                    index = 0;
                }
            } catch (Exception e) {
                log.error("api", e);
            }
        }
        if (index > 0)
            operationResponseList.addAll(this.kuduSession.flush());
        log.info("kudu {}{},{},{} ms",
                new Object[]{Integer.valueOf(operations.size()), Integer.valueOf(flushSize), Integer.valueOf(operationResponseList.size()),
                        Long.valueOf(System.currentTimeMillis() - start)});
        return operationResponseList;
    }

    public void apply(Operation operation) throws KuduException {
        apply(Collections.singletonList(operation));
    }

    private void printResposse(List<OperationResponse> responses) {
        if (responses == null || responses.isEmpty())
            return;
        List<RowError> rowErrors = OperationResponse.collectErrors(responses);
        if (!rowErrors.isEmpty()) {
            log.error("kudu {}{}", Integer.valueOf(rowErrors.size()), rowErrors);
            for (RowError error : rowErrors) {
                Operation op = error.getOperation();
                log.error("kudu error operation: {}, table: {}, row: {}",
                        new Object[]{op.getClass().getSimpleName(), op.getTable().getName(), op.getRow()});
            }
        }
    }

    public synchronized KuduTable getTable(String tableName) throws KuduException {
        if (!tables.containsKey(tableName)) {
            KuduTable table = this.kuduClient.openTable(tableName);
            tables.put(tableName, table);
        }
        return tables.get(tableName);
    }

    public synchronized List<String> getKeyColumns(String tableName) throws KuduException {
        if (!tablesKeys.containsKey(tableName)) {
            KuduTable table = getTable(tableName);
            List<String> keys = new ArrayList<>();
            for (ColumnSchema keyColumn : table.getSchema().getPrimaryKeyColumns())
                keys.add(keyColumn.getName().toUpperCase());
            tablesKeys.put(tableName, keys);
        }
        return tablesKeys.get(tableName);
    }

    public Insert createInsert(String table, Object data) throws KuduException {
        KuduTable ktable = getTable(table);
        return (Insert) fillData(data, ktable, OperationType.INSERT);
    }

    public Update createUpdate(String table, Object data) throws KuduException {
        KuduTable ktable = getTable(table);
        return (Update) fillData(data, ktable, OperationType.UPDATE);
    }

    public Delete createDelete(String table, Object data) throws KuduException {
        KuduTable ktable = getTable(table);
        return (Delete) fillData(data, ktable, OperationType.DELETE);
    }

    public Upsert createUpsert(String table, Object data) throws KuduException {
        KuduTable ktable = getTable(table);
        return (Upsert) fillData(data, ktable, OperationType.UPSERT);
    }

    public void delete(String table, Object data) throws KuduException {
        Delete delete = createDelete(table, data);
        apply((Operation) delete);
    }

    public void insert(String table, Object data) throws KuduException {
        Insert insert = createInsert(table, data);
        apply((Operation) insert);
    }

    public void update(String table, Object data) throws KuduException {
        Update update = createUpdate(table, data);
        apply((Operation) update);
    }

    public void upsert(String table, Object data) throws KuduException {
        Upsert upsert = createUpsert(table, data);
        apply((Operation) upsert);
    }

    private Operation fillData(Object data, KuduTable ktable, OperationType type) {
        boolean delete = OperationType.DELETE.equals(type);
        Operation operation = createOperation(ktable, type);
        PartialRow row = operation.getRow();
        Schema schema = ktable.getSchema();
        Map<String, ColumnSchema> cols = getCols(schema);
        if (data instanceof Map) {
            Map<String, Object> dataMap = (Map<String, Object>) data;
            for (String key : dataMap.keySet()) {
                ColumnSchema columnSchema = cols.get(transColName(key));
                if (columnSchema != null) {
                    if (delete && !columnSchema.isKey()) {
                        log.trace("{}", columnSchema.getName());
                        continue;
                    }
                    fillCol(row, columnSchema, dataMap.get(key));
                }
            }
        } else {
            Class<?> clazz = data.getClass();
            for (Method method : clazz.getMethods()) {
                String methodName = method.getName();
                if (methodName.startsWith("get") && method.getParameterCount() == 0 && !"getClass".equals(methodName)) {
                    ColumnSchema columnSchema = cols.get(transColName(methodName.substring(3)));
                    if (null != columnSchema) {
                        Object value = null;
                        try {
                            value = method.invoke(data, new Object[0]);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }
                        if (null != value)
                            if (delete && !columnSchema.isKey()) {
                                log.info("{}", columnSchema.getName());
                            } else {
                                fillCol(row, columnSchema, value);
                            }
                    }
                }
            }
        }
        return operation;
    }

    private Operation createOperation(KuduTable ktable, OperationType type) {
        Operation operation = null;
        switch (type) {
            case INSERT:
                operation = ktable.newInsert();
                break;
            case UPDATE:
                operation = ktable.newUpdate();
                break;
            case UPSERT:
                operation = ktable.newUpsert();
                break;
            case DELETE:
                operation = ktable.newDelete();
                break;
            default:
                break;
        }
        return operation;
    }

    public Map<String, ColumnSchema> getCols(Schema schema) {
        Map<String, ColumnSchema> data = new HashMap<>();
        for (ColumnSchema columnSchema : schema.getColumns())
            data.put(transColName(columnSchema.getName()), columnSchema);
        return data;
    }

    private String transColName(String colName) {
        return colName.replaceAll("_", "").toLowerCase();
    }

    private static void fillCol(PartialRow row, ColumnSchema colSchema, Object value) {
        String name = colSchema.getName();
        if (null == value) {
            row.setNull(colSchema.getName());
            return;
        }
        Type type = colSchema.getType();
        switch (type) {
            case STRING:
                row.addString(name, TypeUtils.castToString(value));
                break;
            case INT64:
                if (value instanceof Timestamp) {
                    long seconds = TimeUnit.MILLISECONDS.toSeconds(((Timestamp) value).getTime());
                    row.addLong(name, TypeUtils.castToLong(Long.valueOf(seconds)).longValue());
                    break;
                }
                row.addLong(name, TypeUtils.castToLong(value).longValue());
                break;
            case UNIXTIME_MICROS:
                if (value instanceof Timestamp) {
                    long microseconds = ((Timestamp) value).getTime() * 1000L;
                    row.addLong(name, TypeUtils.castToLong(Long.valueOf(microseconds)).longValue());
                    break;
                }
                row.addLong(name, TypeUtils.castToLong(value).longValue());
                break;
            case DOUBLE:
                row.addDouble(name, TypeUtils.castToDouble(value).doubleValue());
                break;
            case INT32:
                row.addInt(name, TypeUtils.castToInt(value).intValue());
                break;
            case INT16:
                row.addShort(name, TypeUtils.castToShort(value).shortValue());
                break;
            case INT8:
                row.addByte(name, TypeUtils.castToByte(value).byteValue());
                break;
            case BOOL:
                row.addBoolean(name, TypeUtils.castToBoolean(value).booleanValue());
                break;
            case BINARY:
                row.addBinary(name, TypeUtils.castToBytes(value));
                break;
            case FLOAT:
                row.addFloat(name, TypeUtils.castToFloat(value).floatValue());
                break;
            case DECIMAL:
                row.addDecimal(name, TypeUtils.castToBigDecimal(value));
                break;
        }
    }
}
