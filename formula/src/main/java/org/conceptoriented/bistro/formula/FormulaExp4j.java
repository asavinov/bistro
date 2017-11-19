package org.conceptoriented.bistro.formula;

import org.conceptoriented.bistro.core.Table;

public class FormulaExp4j extends FormulaBase {

    public FormulaExp4j() {
        super();
        isExp4j = true;
        isEvalex = false;
    }
    public FormulaExp4j(String formula, Table table) {
        super(formula, table);
        isExp4j = true;
        isEvalex = false;

        this.translate(formula);
    }
}
