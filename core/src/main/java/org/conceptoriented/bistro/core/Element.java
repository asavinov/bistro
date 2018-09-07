package org.conceptoriented.bistro.core;

import java.util.List;

public interface Element {

    Table getTable();
    Column getColumn();

    List<Element> getDependencies();
    boolean hasDependency(Element element);

    List<Element> getDependents();
    boolean hasDependents(Element element);

    List<BistroException> getErrors();
    boolean hasErrorsDeep();

    Operation getOperation();
    void setOperation(Operation operation);
    OperationType getOperationType();

    /**
     * Whether dependencies (not this element) have changed their data state (since last evaluation) and hence this element has to be evaluated.
     * This includes also changes of definitions including the definition of this element as well as anything that can influence the data state of this element.
     * This does not include the data state of this element itself.
     */
    boolean isDirty();

    /**
     * Make this element data state up-to-date and consistent with the state of all dependencies by processing new (dirty) data in the dependencies
     */
    void evaluate();
}
