package org.conceptoriented.bistro.core;

import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.conceptoriented.bistro.core.expr.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
        Column ta = s.getColumn("T", "A");
        Column tb = s.getColumn("T", "B");

        // Use formulas
        tb.calculate(UDE.Exp4j, "2 * [A] + 1");
        tb.evaluate();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);

        // Use objects
        List<ColumnPath> paths = Arrays.asList(new ColumnPath(Arrays.asList(ta)));
        tb.calculate(CustomCalcUde.class, paths);
        tb.evaluate();

        assertTrue(tb.getDependencies().contains(ta)); // Check correctness of dependencies

        assertEquals(11.0, (Double) tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double) tb.getValue(2), Double.MIN_VALUE);

        // Use lambda
        tb.calculate((p, o) -> 2.0 * (Double) (p[0] == null ? Double.NaN : p[0]) + 1, paths);
        tb.evaluate();

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
        t2c.link(UDE.Exp4j, Arrays.asList("A", "B"), Arrays.asList("[A]", "[B]")); // [A]=[A]; [B]=[B]
        t2c.evaluate();

        // Check correctness of dependencies
        List<Column> t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(s.getColumn("T2", "A")));
        assertTrue(t2c_deps.contains(s.getColumn("T2", "B")));

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // Appended

        // Use UDE instances
        List<Column> columns = Arrays.asList(s.getColumn("T", "A"), s.getColumn("T", "B"));
        List<UDE> udes = Arrays.asList(new UdeExp4j("[A]", t2), new UdeExp4j("[B]", t2));

        t2c.link(columns, udes);

        // Check correctness of dependencies
        t2c_deps = t2c.getDependencies();
        assertTrue(t2c_deps.contains(s.getColumn("T2", "A")));
        assertTrue(t2c_deps.contains(s.getColumn("T2", "B")));

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
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = s.getColumn("T", "A");
        Column t2g = s.getColumn("T2", "G");

        // Create custom accu expression and bind to certain parameter paths
        UDE accuUde = new CustomAccuUde(Arrays.asList(new ColumnPath(s.getColumn("T2", "Id"))));

        ta.accumulate(new UdeExp4j("0.0", s.getTable("T")), accuUde, null, new ColumnPath(t2g));
        ta.evaluate(); // It has to also evaluate the accu (group) columns

        // Check correctness of dependencies
        List<Column> ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // The same accu expression but translated from a formula
        accuUde = new UdeExp4j(" [out] + 2.0 * [Id] ", s.getTable("T2"));
        ;

        ta.accumulate(new UdeExp4j("0.0", s.getTable("T")), accuUde, null, new ColumnPath(t2g));
        ta.evaluate();

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // The same using formulas
        ta.accumulate(UDE.Exp4j, "0.0", " [out] + 2.0 * [Id] ", null, "T2", new NamePath("G"));
        ta.evaluate();

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue(ta_deps.contains(s.getColumn("T2", "Id"))); // Used in formula
        assertTrue(ta_deps.contains(s.getColumn("T2", "G"))); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // Test how dirty status is propagated through dependencies
        t2.getColumn("Id").setValue(2, 5.0); // Change (make dirty) non-derived column
        s.evaluate(); // Both t2g and ta have to be evaluated
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
        ta.setDefinitionType(DefinitionType.ACCU);

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
        t2g.link(UDE.Exp4j, Arrays.asList("Id"), Arrays.asList("[Id]"));

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


class CustomCalcUde implements UDE {

    @Override public void setParamPaths(List<NamePath> paths) {}
    @Override public List<NamePath> getParamPaths() { return null; }

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
    @Override public void setResolvedParamPaths(List<ColumnPath> paths) { this.inputPaths.addAll(paths); }
    @Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    @Override public Object evaluate(Object[] params, Object out) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        return 2.0 * param + 1.0; // "2 * [A] + 1"
    }
    public CustomCalcUde() {
    }
    public CustomCalcUde(List<ColumnPath> inputPaths) {
        this.setResolvedParamPaths(inputPaths);
    }
}

class CustomAccuUde implements UDE {

    @Override public void setParamPaths(List<NamePath> paths) {}
    @Override public List<NamePath> getParamPaths() { return null; }

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
    @Override public void setResolvedParamPaths(List<ColumnPath> paths) { this.inputPaths.addAll(paths); }
    @Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    @Override public Object evaluate(Object[] params, Object out) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        double outVal = out == null ? Double.NaN : ((Number)out).doubleValue();
        return outVal + 2.0 * param; // " [out] + 2.0 * [Id] "
    }
    public CustomAccuUde() {
    }
    public CustomAccuUde(List<ColumnPath> inputPaths) {
        this.setResolvedParamPaths(inputPaths);
    }
}
