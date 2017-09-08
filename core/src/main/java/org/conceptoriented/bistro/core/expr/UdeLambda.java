package org.conceptoriented.bistro.core.expr;

import java.util.ArrayList;
import java.util.List;

import org.conceptoriented.bistro.core.*;

// This class takes lambda as a parameter and then uses it for evaluation
public class UdeLambda implements UDE {

    @Override public void setParamPaths(List<NamePath> paths) {}
    @Override public List<NamePath> getParamPaths() { return null; }

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
    @Override public void setResolvedParamPaths(List<ColumnPath> paths) { this.inputPaths.addAll(paths); }
    @Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    @Override public void translate(String formula) {}
    @Override public List<BistroError> getTranslateErrors() { return null; }

    UdeEvaluate evaluateLambda;
    @Override public Object evaluate(Object[] params, Object out) {
        return evaluateLambda.evaluate(params, out);
    }
    @Override public BistroError getEvaluateError() { return null; }

    public UdeLambda() {
    }
    public UdeLambda(UdeEvaluate evaluateLambda) {
        this.evaluateLambda = evaluateLambda;
    }
    public UdeLambda(UdeEvaluate evaluateLambda, List<ColumnPath> inputPaths) {
        this.evaluateLambda = evaluateLambda;
        this.setResolvedParamPaths(inputPaths);
    }
}
