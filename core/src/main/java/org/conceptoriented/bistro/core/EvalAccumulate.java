package org.conceptoriented.bistro.core;

/**
 * This methods knows how to update an aggregate given a fact (group member) described by its parameters.
 */
@FunctionalInterface
public interface EvalAccumulate {
    public Object evaluate(Object aggregate, Object[] params) throws BistroException;
}
