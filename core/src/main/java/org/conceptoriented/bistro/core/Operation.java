package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * Objects of this class know how to produce (derive, infer) new data from existing (input) data.
 * In particular, it knows what subsets to iterate through, how to read inputs and how to write output values.
 */
public interface Operation {
    public List<BistroError> getErrors();
    public List<Element> getDependencies();
    public void evaluate();
}
