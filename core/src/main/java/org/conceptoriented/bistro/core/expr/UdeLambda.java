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

    UdeEvaluate evaluateLambda;
    @Override public Object evaluate(Object[] params, Object out) throws BistroError {
        return evaluateLambda.evaluate(params, out);
    }

    public static UdeLambda createInstance(UdeEvaluate lambda) throws BistroError {
        UdeLambda ude = new UdeLambda(lambda);
        if(lambda == null) {
            throw new BistroError(BistroErrorCode.DEFINITION_ERROR, "Error creating expression", "Lambda cannot be null.");
        }
        return ude;
    }

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
