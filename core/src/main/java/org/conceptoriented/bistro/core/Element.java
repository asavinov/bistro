package org.conceptoriented.bistro.core;

import java.util.List;

public interface Element {

    public Table getTable();
    public Column getColumn();

    public List<Element> getDependencies();
    public boolean hasDependency(Element element);

    public List<Element> getDependents();
    public boolean hasDependents(Element element);

    public List<BistroException> getErrors();
    public boolean hasErrorsDeep();

    public Operation getOperation();
    public OperationType getOperationType();

    // Having changed flag means that there have been SOME changes in this element
    // In the case of no additional information about the scope of changes (delta), we assume that changes can be anywhere in this element and normally this leads to full re-evaluation of dependents
    // Columns do not set this flag in data methods (by assuming that changes are only in newly added records). If necessary, the flag has to be set manually.
    // Tables set this flag in data methods as well as register the scope of changes automatically.
    public long getChangedAt();
    public boolean isChanged();
    public void setChanged();
    public void resetChanged(); // Forget the changes. Normally after evaluation of all dependents.

    public boolean isDirty(); // Based on dependencies

    public void evaluate(); // Evaluate
}
