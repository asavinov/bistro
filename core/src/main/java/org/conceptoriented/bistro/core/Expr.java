package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class is a basic implementation of the Expression interface.
 * It can be extended to implement custom expressions.
 */
@Deprecated
public class Expr implements Expression {

    @Override
    public EvaluatorCalc getEvaluator() { return this.evaluateLambda; };

    List<ColumnPath> parameterPaths = new ArrayList<>();
    @Override public void setParameterPaths(List<ColumnPath> paths) { this.parameterPaths.addAll(paths); }
    @Override public List<ColumnPath> getParameterPaths() { return parameterPaths; }

    EvaluatorCalc evaluateLambda;
    @Override public Object evaluate(Object[] params) throws BistroError {
        return evaluateLambda.evaluate(params);
    }

    public Expr(EvaluatorCalc eval, ColumnPath... params) {
        this.evaluateLambda = eval;

        if(params == null) params = new ColumnPath[]{};
        this.setParameterPaths(Arrays.asList(params));
    }
    public Expr(EvaluatorCalc eval, Column... params) {
        this.evaluateLambda = eval;

        if(params == null) params = new Column[]{};
        List<ColumnPath> paths = new ArrayList<>();
        for(int i=0; i<params.length; i++) {
            paths.add(new ColumnPath(params[i]));
        }
        this.setParameterPaths(paths);
    }
}
