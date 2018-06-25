package org.conceptoriented.bistro.core;

/**
 * This method knows how to compute one output value given several input values as an array.
 */
@FunctionalInterface
public interface EvalCalculate {
    public Object evaluate(Object[] params) throws BistroException;
}
