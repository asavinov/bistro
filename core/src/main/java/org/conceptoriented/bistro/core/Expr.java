package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
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

    public Expr(Evaluator eval, ColumnPath[] params) {
        this.evaluateLambda = eval;
        this.setParameterPaths(Arrays.asList(params));
    }
    public Expr(Evaluator eval, Column[] params) {
        this.evaluateLambda = eval;

        List<ColumnPath> paths = new ArrayList<>();
        for(int i=0; i<params.length; i++) {
            paths.add(new ColumnPath(params[i]));
        }
        this.setParameterPaths(paths);
    }
}
