package org.conceptoriented.bistro.core;

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
