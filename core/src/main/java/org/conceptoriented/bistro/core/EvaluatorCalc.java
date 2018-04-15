package org.conceptoriented.bistro.core;

/**
 * This method knows how to compute one output value given several input values as an array.
 */
@FunctionalInterface
public interface EvaluatorCalc {
    public Object evaluate(Object[] params) throws BistroError;
}
