package org.conceptoriented.bistro.core;

import java.util.List;

public interface Element {

    public Table getTable();
    public Column getColumn();

    public List<Element> getDependencies();
    public List<Element> getDependants();

    public List<BistroError> getExecutionErrors();
    public boolean hasExecutionErrorsDeep();

    public List<BistroError> getDefinitionErrors();
    public boolean hasDefinitionErrorsDeep();

    public boolean isDirty();
    public void setDirty();

    public boolean isDirtyDeep(); // Dirty status of this and all dependencies
    //public void setDirtyDeep(); // Dirty status of this and all dependants

    public void run();
}
