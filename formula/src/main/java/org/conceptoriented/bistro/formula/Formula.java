package org.conceptoriented.bistro.formula;

import org.conceptoriented.bistro.core.ColumnPath;
import org.conceptoriented.bistro.core.EvalCalculate;

import java.util.List;

public interface Formula extends Expression {
    public static String Exp4j = "org.conceptoriented.bistro.core.formula.FormulaExp4j";
    public static String Evalex = "org.conceptoriented.bistro.core.formula.FormulaEvalex";
    public static String Mathparser = "org.conceptoriented.bistro.core.formula.FormulaMathparser";
    public static String JavaScript = "org.conceptoriented.bistro.core.formula.FormulaJavaScript";
}
