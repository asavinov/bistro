package org.conceptoriented.bistro.core;

import java.util.List;
import java.util.Map;

public interface TableData {


    long add();

    Range add(long count);

    long remove();

    Range remove(long count);

    void removeAll();

    long remove(Column column, Object value);

    // Initialize to default state (e.g., empty set) by also forgetting change history
    // It is important to propagate this operation to all dependents as reset (not simply emptying) because some of them (like accumulation) have to forget/reset history and ids/references might become invalid
    // TODO: This propagation can be done manually or we can introduce a special method or it a special reset flag can be introduced which is then inherited and executed by all dependents during evaluation.
    void reset();

    //
    // Read/write records (convenience methods)
    //
    void getValues(long id, Map<String,Object> record);
    void getValues(long id, List<Column> columns, List<Object> values);
    void setValues(long id, Map<String,Object> record);
    void setValues(long id, List<Column> columns, List<Object> values);

    // Find id with the specified column values.
    // If many records satisfy the criteria then the id returned is not determined (any can be returned).
    // If not found, then return negative id.
    // Important: Values must have the same type as the column data type - otherwise the comparision will not work
    // ISSUE: If not found, should output be NULL or -1? On one hand, we say that links are Long. But Long can be NULL. In future, it could be long which cannot be NULL.
    long findValues(List<Object> values, List<Column> columns);

    //
    // Tracking changes.
    //

    Range getAddedRange();

    Range getRemovedRange();

    Range getIdRange();

    long getLength();

    long getChangedAt();

    boolean isChanged();

    void setChanged();

    void resetChanged();
}
