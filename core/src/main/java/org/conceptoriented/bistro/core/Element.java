package org.conceptoriented.bistro.core;

import java.util.List;

public interface Element {

    public Table getTable();
    public Column getColumn();

    public List<Element> getDependencies();
    public boolean hasDependency(Element element);

    public List<Element> getDependents();
    public boolean hasDependents(Element element);

    public List<BistroError> getExecutionErrors();
    public boolean hasExecutionErrorsDeep();

    public List<BistroError> getDefinitionErrors();
    public boolean hasDefinitionErrorsDeep();

    public boolean isChanged();
    public void setChanged(); // Normally is not used directly - only from data manipulation methods
    public void resetChanged(); // Normally after evaluation (when the delta has been propagated and not needed anymore)
    public boolean isChangedDependencies(); // Based on dependencies

    public void run(); // Evaluate
}
