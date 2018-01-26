package org.conceptoriented.bistro.core;

/**
 * This method knows how to compute one output value given several input values as an array and one optional current output value (for accumulation).
 */
@FunctionalInterface
public interface Evaluator {
    public Object eval(Object[] params) throws BistroError;
}
