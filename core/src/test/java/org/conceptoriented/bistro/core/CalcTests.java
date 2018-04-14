package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CalcTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void calcTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("T");
        Column ta = t.getColumn("A");
        Column tb = t.getColumn("B");

        // Lambda
        tb.calc(
                p -> 2.0 * (Double) (p[0] == null ? Double.NaN : p[0]) + 1,
                ta
        );
        tb.evaluate();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);

        // Expression
        Expression expr = new CustomCalcExpr(new ColumnPath(ta));

        tb.calc(expr);
        tb.evaluate();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);
    }

    Schema createSchema() {
        Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", t);
        Column tb = s.createColumn("B", t);

        t.add();
        t.add();
        t.add();
        ta.setValue(0, 5.0);
        ta.setValue(1, null);
        ta.setValue(2, 6.0);

        return s;
    }

}

class CustomCalcExpr implements Expression {

    List<ColumnPath> parameterPaths = new ArrayList<>(); // The expression parameters are bound to these input column valuePaths
    @Override public void setParameterPaths(List<ColumnPath> paths) { this.parameterPaths.addAll(paths); }
    @Override public List<ColumnPath> getParameterPaths() { return parameterPaths; }

    @Override public Object eval(Object[] params) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        return 2.0 * param + 1.0; // "2 * [A] + 1"
    }

    public CustomCalcExpr() {
    }
    public CustomCalcExpr(ColumnPath... params) {
        this.setParameterPaths(Arrays.asList(params));
    }
}
