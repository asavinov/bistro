package org.conceptoriented.bistro.core;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    @Before
    public void setUp() {
    }

    @Test
    public void schemaTest() // Schema operations
    {
        // Create and configure: schema, tables, columns
        Schema s = new Schema("My Schema");

        Table t = s.createTable("T");

        Column c1 = s.createColumn("A", t);
        Column c2 = s.createColumn("B", t);
        Column c3 = s.createColumn("C", t);

        s.deleteColumn(c2);
        assertEquals(2, t.getColumns().size());

        s.deleteTable(t);
        assertEquals(1, s.getTables().size()); // Only primitive tables
    }

    /**
     * Principles:
     * Elements are added/remove using table methods.
     * The range of ids is read from the table.
     * Values are set and get using column methos.
     * Convenience methods:
     * Getting or setting several columns simultaniously using Map (of column names or column references).
     */
    @Test
    public void dataTest() { // Manual operations table id ranges
        // Prepopulate table for experiments with column data
        Schema s = new Schema("My Schema");
        Table t = s.createTable("My Table");
        Column c1 = s.createColumn("My Column", t);

        long id = t.add();
        assertEquals(0, id);
        long id2 = t.add();
        c1.setValue(id2, 1.0);
        assertEquals(1.0, (double) c1.getValue(id2), Double.MIN_VALUE);

        Column c2 = s.createColumn("My Column 2", t);
        long id3 = t.add();
        c2.setValue(id3, "StringValue");
        assertEquals("StringValue", (String) c2.getValue(id3));

        assertEquals(0, t.getIdRange().start);
        assertEquals(3, t.getIdRange().end);

        // Records.
        // Working with multiple columns (records)
        long id4 = t.add();
        List<Column> cols = Arrays.asList(c1, c2);
        List<Object> vals = Arrays.asList(2.0, "StringValue 2");
        t.setValues(id4, cols, vals);
        assertEquals(2.0, (double) c1.getValue(id4), Double.MIN_VALUE);
        assertEquals("StringValue 2", (String) c2.getValue(id4));

        // Record search
        vals = Arrays.asList(2.0, "StringValue 2");
        long found_id = t.find(cols, vals, false); // Record exist
        assertEquals(id4, found_id);

        vals = Arrays.asList(2.0, "Non-existing value");
        found_id = t.find(cols, vals, false); // Record does not exist
        assertTrue(found_id < 0);

        vals = Arrays.asList(5.0, "String value 5"); // Record does not exist
        found_id = t.find(cols, vals, true); // Add if not found
        assertEquals(4, found_id);

        vals = Arrays.asList(5L, "String value 5"); // Record exist but we specify different type (integer instead of double)
        found_id = t.find(cols, vals, false);
        assertTrue(found_id < 0); // Not found because of different types: Long is not comparable with Double
    }

    @Test
    public void calcTest() {
        Schema s = this.createCalcSchema();
        Table t = s.getTable("T");
        Column ta = t.getColumn("A");
        Column tb = t.getColumn("B");

        // Lambda
        tb.calc(
                p -> 2.0 * (Double) (p[0] == null ? Double.NaN : p[0]) + 1,
                ta
        );
        tb.eval();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);

        // Expression
        Expression expr = new CustomCalcExpr(new ColumnPath(ta));

        tb.calc(expr);
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

        // Use column paths
        Column[] columns = new Column[] {t.getColumn("A"), t.getColumn("B")};

        t2c.link(
                columns, // A and B from T
                t2.getColumn("A"), t2.getColumn("B") // A and B from T2
        );
        t2c.eval();

        // Check correctness of dependencies
        List<Column> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t2.getColumn("B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // Exists. Has been appended before

        t2c.link(
                columns,
                new Expr( p -> p[0], t2.getColumn("A") ), // This expression computers values for "A"
                new Expr( p -> p[0], t2.getColumn("B") ) // This expression computers values for "B"
        );
        t2c.eval();

        // Check correctness of dependencies
        t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(t2.getColumn("A")));
        assertTrue(t2c_deps.contains(t2.getColumn("B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // Exists. Has been appended before
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
        Table t = s.getTable("T");
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = t.getColumn("A");
        Column t2g = t2.getColumn("G");

        // Lambda for accumulation " [out] + 2.0 * [Id] "
        ta.setDefaultValue(0.0);
        ta.accu(
                t2g,
                p -> 2.0 * (Double)p[0] + (Double)p[1],
                t2.getColumn("Id")
        );
        ta.eval();

        // Check correctness of dependencies
        List<Column> ta_deps = ta.getDependencies();
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
        ta.eval(); // It has to also eval the accu (group) columns

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(t2.getColumn("Id"))); // Used in formula
        assertTrue(ta_deps.contains(t2.getColumn("G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }

    Schema createAccuSchema() {

        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t = schema.createTable("T");

        Column tid = schema.createColumn("Id", t);

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
                new Column[] { tid }, // Id from T
                new Column[] { t2id } // Id from T2
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


class CustomCalcExpr implements Expression {

    List<ColumnPath> parameterPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
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

class CustomAccuExpr implements Expression {

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
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
