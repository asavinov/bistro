package org.conceptoriented.bistro.formula;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.conceptoriented.bistro.core.*;

/**
 * This class is a basic implementation of the Expression interface.
 * It can be extended to implement custom expressions.
 */
public class Expr implements Expression {

    @Override
    public EvalCalculate getEvaluator() { return this.evaluateLambda; };

    List<ColumnPath> parameterPaths = new ArrayList<>();
    @Override public void setParameterPaths(List<ColumnPath> paths) { this.parameterPaths.addAll(paths); }
    @Override public List<ColumnPath> getParameterPaths() { return parameterPaths; }

    EvalCalculate evaluateLambda;
    @Override public Object evaluate(Object[] params) throws BistroException {
        return evaluateLambda.evaluate(params);
    }

    public Expr(EvalCalculate eval, ColumnPath... params) {
        this.evaluateLambda = eval;

        if(params == null) params = new ColumnPath[]{};
        this.setParameterPaths(Arrays.asList(params));
    }
    public Expr(EvalCalculate eval, Column... params) {
        this.evaluateLambda = eval;

        if(params == null) params = new Column[]{};
        List<ColumnPath> paths = new ArrayList<>();
        for(int i=0; i<params.length; i++) {
            paths.add(new ColumnPath(params[i]));
        }
        this.setParameterPaths(paths);
    }
}
