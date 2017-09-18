package org.conceptoriented.bistro.core;

/**
 * This method knows how to compute one output value given several input values as an array and one optional current output value (for accumulation).
 * It can be provided as a lambda function or implemented in a class.
 * The eval method is unaware of where the inputs and the output value come from (that is, it is unaware of columns).
 */
@FunctionalInterface
public interface Evaluator {
    public Object evaluate(Object[] params, Object out) throws BistroError;
}
