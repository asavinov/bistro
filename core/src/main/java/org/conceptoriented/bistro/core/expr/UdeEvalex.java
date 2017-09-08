package org.conceptoriented.bistro.core.expr;

import org.conceptoriented.bistro.core.Table;

public class UdeEvalex extends UdeJava {
    public UdeEvalex() {
        super();
        isExp4j = false;
        isEvalex = true;
    }
    public UdeEvalex(String formula, Table table) {
        super(formula, table);
        isExp4j = false;
        isEvalex = true;

        this.translate(formula);
    }
}
