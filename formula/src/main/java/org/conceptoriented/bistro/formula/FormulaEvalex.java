package org.conceptoriented.bistro.formula;

import org.conceptoriented.bistro.core.Table;

import java.math.BigDecimal;
import java.util.List;

public class FormulaEvalex extends FormulaBase {

    public static void evalexTest()
    {
        BigDecimal result = null;

        com.udojava.evalex.Expression e = new com.udojava.evalex.Expression("1+1/3");
        e.setPrecision(2);
        e.setRoundingMode(java.math.RoundingMode.UP);
        result = e.eval();

        e = new com.udojava.evalex.Expression("SQRT(a^2 + b^2)");
        List<String> usedVars = e.getUsedVariables();

        e.getExpressionTokenizer(); // Does not detect definitionErrors

        e.setVariable("a", "2.4"); // Can work with strings (representing numbers)
        e.setVariable("b", new BigDecimal(9.253));

        // Validate
        try {
            e.toRPN(); // Generates prefixed representation but can be used to check definitionErrors (variables have to be set in order to correctly determine parse definitionErrors)
        }
        catch(com.udojava.evalex.Expression.ExpressionException ee) {
            System.out.println(ee);
        }

        result = e.eval();

        result = new com.udojava.evalex.Expression("random() > 0.5").eval();

        //e = new com.udojava.evalex.Expr("MAX('aaa', 'bbb')");
        // We can define custom functions but they can take only numbers (as constants).
        // EvalEx does not have string parameters (literals).
        // It does not recognize quotes. So maybe simply introduce string literals even if they will be converted into numbers, that is, just like string in setVariable.
        // We need to change tokenizer by adding string literals in addition to numbers and then their processing.

        e.eval();
    }

    public FormulaEvalex() {
        super();
        isExp4j = false;
        isEvalex = true;
    }
    public FormulaEvalex(String formula, Table table) {
        super(formula, table);
        isExp4j = false;
        isEvalex = true;

        this.translate(formula);
    }
}
