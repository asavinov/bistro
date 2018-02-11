package org.conceptoriented.bistro.core;

/**
 * This methods knows how to update aggregate given a group member described by parameters and its distance from the group center.
 */
@FunctionalInterface
public interface EvaluatorRoll {
    public Object eval(Object aggregate, double distance, Object[] params) throws BistroError;
}
