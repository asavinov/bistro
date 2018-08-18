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

    public boolean isDirty(); // Based on dependencies

    public void evaluate(); // Evaluate
}
