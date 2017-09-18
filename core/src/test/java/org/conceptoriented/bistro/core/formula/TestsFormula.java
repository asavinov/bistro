package org.conceptoriented.bistro.core.formula;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.core.formula.*;
import org.conceptoriented.bistro.core.formula.FormulaExp4J;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestsFormula {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void calcTest() {
        Schema s = this.createCalcSchema();
        Column ta = s.getColumn("T", "A");
        Column tb = s.getColumn("T", "B");

        // Use formulas
        tb.calc(Formula.Exp4j, "2 * [A] + 1");
        tb.eval();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);
    }

    Schema createCalcSchema() {
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


    @Test
    public void linkTest() {
        Schema s = createLinkSchema();
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");
        Column t2c = t2.getColumn("C");

        // Use formulas
        t2c.link(Formula.Exp4j, Arrays.asList("A", "B"), Arrays.asList("[A]", "[B]")); // [A]=[A]; [B]=[B]

        t2c.eval();

        // Check correctness of dependencies
        List<Column> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(s.getColumn("T2", "A")));
        assertTrue(t2c_deps.contains(s.getColumn("T2", "B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // Appended

        // Using individual expressions creaed from formulas
        List<Column> columns = Arrays.asList(t.getColumn("A"), t.getColumn("B"));
        List<Expression> exprs = exprs = Arrays.asList(
                new FormulaExp4J("[A]", t2),
                new FormulaExp4J("[B]", t2)
        );

        t2c.link(columns, exprs);

        t2c.eval();
    }

    Schema createLinkSchema() {
        // Create and configure: schema, tables, columns
        Schema s = new Schema("My Schema");

        //
        // Table 1 (type table to link to)
        //
        Table t1 = s.createTable("T");

        Column t1a = s.createColumn("A", t1);
        Column t1b = s.createColumn("B", t1);

        // Add one record to link to
        t1.add();
        t1a.setValue(0, 5.0);
        t1b.setValue(0, "bbb");

        //
        // Table 2 (referencing table to link records from)
        //
        Table t2 = s.createTable("T2");

        Column t2a = s.createColumn("A", t2);
        Column t2b = s.createColumn("B", t2);

        Column t2c = s.createColumn("C", t2, t1);

        // Add two records to link from
        t2.add();
        t2a.setValue(0, 5.0);
        t2b.setValue(0, "bbb");
        t2.add();
        t2a.setValue(1, 10.0);
        t2b.setValue(1, "ccc");

        return s;
    }


    @Test
    public void accuTest() {
        Schema s = this.createAccuSchema();
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = s.getColumn("T", "A");
        Column t2g = s.getColumn("T2", "G");

        // Accu expression translated from a formula
        Expression accuExpr = new FormulaExp4J(" [out] + 2.0 * [Id] ", s.getTable("T2"));

        ta.accu(new FormulaExp4J("0.0", s.getTable("T")), accuExpr, null, new ColumnPath(t2g));
        ta.eval();

        // Check correctness of dependencies
        List<Column> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // The same using formulas
        ta.accu(Formula.Exp4j, "0.0", " [out] + 2.0 * [Id] ", null, "T2", new NamePath("G"));
        ta.eval();

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // Test how dirty status is propagated through dependencies
        t2.getColumn("Id").setValue(2, 5.0); // Change (make dirty) non-derived column
        s.eval(); // Both t2g and ta have to be evaluated
        assertEquals(30.0, ta.getValue(0));
    }

    Schema createAccuSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("Id", t1);

        // Define accu column
        Column ta = schema.createColumn("A", t1);
        ta.setDefinitionType(ColumnDefinitionType.ACCU);

        t1.add();
        t1.add();
        t1.add();
        tid.setValue(0, 5.0);
        tid.setValue(1, 10.0);
        tid.setValue(2, 15.0);

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("Id", t2);

        // Define group column
        Column t2g = schema.createColumn("G", t2, t1);
        t2g.link(Formula.Exp4j, Arrays.asList("Id"), Arrays.asList("[Id]"));

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
