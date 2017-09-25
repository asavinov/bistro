package org.conceptoriented.bistro.formula;

import org.conceptoriented.bistro.core.Table;

public class FormulaExp4J extends FormulaBase {

    public FormulaExp4J() {
        super();
        isExp4j = true;
        isEvalex = false;
    }
    public FormulaExp4J(String formula, Table table) {
        super(formula, table);
        isExp4j = true;
        isEvalex = false;

        this.translate(formula);
    }
}
