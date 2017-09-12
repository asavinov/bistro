package org.conceptoriented.bistro.core.expr;

import org.conceptoriented.bistro.core.BistroError;

@FunctionalInterface
public interface UdeEvaluate {
    public Object evaluate(Object[] params, Object out) throws BistroError;
}
