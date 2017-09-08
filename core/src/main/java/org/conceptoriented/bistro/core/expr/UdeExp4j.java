package org.conceptoriented.bistro.core.expr;

import org.conceptoriented.bistro.core.Table;

public class UdeExp4j extends UdeJava {
    public UdeExp4j() {
        super();
        isExp4j = true;
        isEvalex = false;
    }
    public UdeExp4j(String formula, Table table) {
        super(formula, table);
        isExp4j = true;
        isEvalex = false;

        this.translate(formula);
    }
}
