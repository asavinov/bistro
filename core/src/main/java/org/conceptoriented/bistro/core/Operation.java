package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * Objects of this class know how to produce (derive, infer) new data from existing (input) data.
 * It knows what subsets to iterate through, how to (optimally) iterate, how to read inputs
 * and how to write produced output values back to the data state.
 */
public interface Operation {
    public OperationType getOperationType();
    public List<Element> getDependencies();
    public void evaluate();
}
