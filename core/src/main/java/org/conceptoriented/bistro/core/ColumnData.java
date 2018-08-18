package org.conceptoriented.bistro.core;

public interface ColumnData {

    //
    // Output values
    //

    public Object getValue(long id);
    public void setValue(long id, Object value); // One id

    public void setValue(Range range, Object value); // Range of ids
    public void setValue(Range range); // Default value
    public void setValue(Object value); // All ids
    public void setValue(); // Default value

    public Object getDefaultValue();
    public void setDefaultValue(Object value);

    //
    // Input range
    //

    public void add();
    public void add(long count); // Remove the oldest records with lowest ids
    public void remove();
    public void remove(long count); // Remove the specified number of oldest records
    public void removeAll();
    public void reset(long start, long end);
    public void gc();

    public long findSorted(Object value);
    public long findSortedFromStart(Object value);

    //
    // Tracking changes (delta)
    //

    // Having changed flag means that there have been SOME changes in this element
    // In the case of no additional information about the scope of changes (delta), we assume that changes can be anywhere in this element and normally this leads to full re-evaluation of dependents
    // Columns do not set this flag in data methods (by assuming that changes are only in newly added records). If necessary, the flag has to be set manually.
    // Tables set this flag in data methods as well as register the scope of changes automatically.
    public long getChangedAt();
    public void setChangedAt(long changedAt);
    public boolean isChanged();
    public void setChanged();
    public void resetChanged(); // Forget the changes. Normally after evaluation of all dependents.
}
