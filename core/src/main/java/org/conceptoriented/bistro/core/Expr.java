package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is a basic implementation of the Expression interface.
 * It can be extended to impelment custom expressions.
 */
public class Expr implements Expression {

    List<ColumnPath> parameterPaths = new ArrayList<>();
    @Override public void setParameterPaths(List<ColumnPath> paths) { this.parameterPaths.addAll(paths); }
    @Override public List<ColumnPath> getParameterPaths() { return parameterPaths; }

    Evaluator evaluateLambda;
    @Override public Object evaluate(Object[] params, Object out) throws BistroError {
        return evaluateLambda.evaluate(params, out);
    }

    public Expr(Evaluator eval, List<ColumnPath> parameterPaths) {
        this.evaluateLambda = eval;
        this.setParameterPaths(parameterPaths);
    }
}
