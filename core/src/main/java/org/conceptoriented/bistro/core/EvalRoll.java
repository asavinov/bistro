package org.conceptoriented.bistro.core;

/**
 * This methods knows how to update an aggregate given a fact (group member) described by its parameters and its distance from the group center.
 */
@FunctionalInterface
public interface EvalRoll {
    public Object evaluate(Object aggregate, double distance, Object[] params) throws BistroException;
}
