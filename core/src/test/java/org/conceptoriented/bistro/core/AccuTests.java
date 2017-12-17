package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccuTests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void accuTest() {
        Schema s = this.createSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = t.getColumn("A");
        Column t2g = t2.getColumn("G");

        t2g.eval();

        // Lambda for accumulation " [out] + 2.0 * [Id] "
        ta.setDefaultValue(0.0);
        ta.accu(
                t2g,
                p -> 2.0 * (Double)p[0] + (Double)p[1],
                t2.getColumn("Id")
        );
        ta.eval();

        // Check correctness of dependencies
        List<Element> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(t2.getColumn("Id"))); // Used in formula
        assertTrue(ta_deps.contains(t2.getColumn("G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));

        // Expression
        ta.accu(
                new ColumnPath(t2g),
                new CustomAccuExpr(new ColumnPath(t2.getColumn("Id")))
        );
        s.eval(); // It has to also eval the accu (group) keyColumns

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(t2.getColumn("Id"))); // Used in formula
        assertTrue(ta_deps.contains(t2.getColumn("G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }

    Schema createSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t = schema.createTable("T");

        Column tid = schema.createColumn("Id", t);
        tid.noop(true);

        // Define accu column
        Column ta = schema.createColumn("A", t);
        ta.setDefinitionType(ColumnDefinitionType.ACCU);

        t.add();
        t.add();
        t.add();
        tid.setValue(0, 5.0);
        tid.setValue(1, 10.0);
        tid.setValue(2, 15.0);

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", t2);

        // Define group column: G: T2 -> T
        Column t2g = schema.createColumn("G", t2, t);
        t2g.link(
                new Column[] { t2id }, // Values: Id from T2
                new Column[] { tid } // Keys: Id from T
        );

        t2.add();
        t2.add();
        t2.add();
        t2.add();
        t2id.setValue(0, 5.0);
        t2id.setValue(1, 5.0);
        t2id.setValue(2, 10.0);
        t2id.setValue(3, 20.0);

        return schema;
    }

}

class CustomAccuExpr implements Expression {

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column valuePaths
    @Override public void setParameterPaths(List<ColumnPath> paths) { this.inputPaths.addAll(paths); }
    @Override public List<ColumnPath> getParameterPaths() { return inputPaths; }

    @Override public Object eval(Object[] params) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        double outVal = params[1] == null ? Double.NaN : ((Number)params[1]).doubleValue();
        return 2.0 * param + outVal; // " 2.0 * [Id] + [out] "
    }
    public CustomAccuExpr() {
    }
    public CustomAccuExpr(ColumnPath... params) {
        this.setParameterPaths(Arrays.asList(params));
    }
}
