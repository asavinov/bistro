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

        Column c1 = s.createColumn("A", t, s.getTable("Double"));
        Column c2 = s.createColumn("B", t, s.getTable("String"));
        Column c3 = s.createColumn("C", t, s.getTable("Double"));

        s.deleteColumn(c2);
        assertEquals(2, t.getColumns().size());

        s.deleteTable(t);
        assertEquals(2, s.getTables().size());

        // Column type rules and principles.
        // Where do we have them? Enums, names, predefined Table classes. Schema initialization with all primitive tables.
        // Are custom primitive tables allowed? What does it mean to declare itself primitive table? Only name? Or some implementation?
        // Check impossibility to create a custom Table with primitive name (name uniqueness?)
        // Get range, add, remove of primitive types.

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
        Column c1 = s.createColumn("My Column", "My Table", "Double"); // <-- Should not we objects? Name use should be limited, indeed.

        long id = t.add();
        assertEquals(0, id);
        long id2 = t.add();
        c1.setValue(id2, 1.0);
        assertEquals(1.0, (double)c1.getValue(id2), Double.MIN_VALUE);

        Column c2 = s.createColumn("My Column 2", "My Table", "String");
        long id3 = t.add();
        c2.setValue(id3, "StringValue");
        assertEquals("StringValue", (String)c2.getValue(id3));

        assertEquals(0, t.getIdRange().start);
        assertEquals(3, t.getIdRange().end);

        // Records.
        // Working with multiple columns (records)
        long id4 = t.add();
        List<Column> cols = Arrays.asList(c1,c2);
        List<Object> vals = Arrays.asList(2.0,"StringValue 2");
        t.setValues(id4, cols, vals);
        assertEquals(2.0, (double)c1.getValue(id4), Double.MIN_VALUE);
        assertEquals("StringValue 2", (String)c2.getValue(id4));

        // Record search
        vals = Arrays.asList(2.0,"StringValue 2");
        long found_id = t.find(cols, vals, false); // Record exist
        assertEquals(id4, found_id);

        vals = Arrays.asList(2.0,"Non-existing value");
        found_id = t.find(cols, vals, false); // Record does not exist
        assertTrue(found_id < 0);

        vals = Arrays.asList(5.0,"String value 5"); // Record does not exist
        found_id = t.find(cols, vals, true); // Add if not found
        assertEquals(4, found_id);

        vals = Arrays.asList(5L,"String value 5"); // Record exist but we specify different type (integer instead of double)
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

        assertTrue( tb.getDependencies().contains(ta) ); // Check correctness of dependencies

        assertEquals(11.0, (Double)tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double)tb.getValue(2), Double.MIN_VALUE);

        // Use objects
        List<ColumnPath> paths = Arrays.asList( new ColumnPath(Arrays.asList(ta)) );
        tb.calculate(CustomCalcUde.class, paths);
        tb.evaluate();

        assertTrue( tb.getDependencies().contains(ta) ); // Check correctness of dependencies

        assertEquals(11.0, (Double)tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double)tb.getValue(2), Double.MIN_VALUE);

        // Use lambda
        tb.calculate( (p,o) -> 2.0*(Double)(p[0]==null?Double.NaN:p[0]) + 1, paths);
        tb.evaluate();

        assertTrue( tb.getDependencies().contains(ta) ); // Check correctness of dependencies

        assertEquals(11.0, (Double)tb.getValue(0), Double.MIN_VALUE);
        assertEquals(Double.NaN, tb.getValue(1));
        assertEquals(13.0, (Double)tb.getValue(2), Double.MIN_VALUE);
    }

    Schema createCalcSchema() {
    	Schema s = new Schema("My Schema");
        Table t = s.createTable("T");
        Column ta = s.createColumn("A", "T", "Double");
        Column tb = s.createColumn("B", "T", "Double");

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
        t2c.link(UDE.Exp4j, Arrays.asList("A","B"), Arrays.asList("[A]","[B]")); // [A]=[A]; [B]=[B]
        t2c.evaluate();

        // Check correctness of dependencies
        List<Column> t2c_deps = t2c.getDependencies();
        assertTrue( t2c_deps.contains( s.getColumn("T2", "A") ) );
        assertTrue( t2c_deps.contains( s.getColumn("T2", "B") ) );

        assertEquals(0L, t2c.getValue(0)); // Existing
        assertEquals(1L, t2c.getValue(1)); // Appended

        // Use UDE instances
        List<Column> columns = Arrays.asList( s.getColumn("T", "A"), s.getColumn("T", "B") );
        List<UDE> udes = Arrays.asList(new UdeExp4j("[A]", t2), new UdeExp4j("[B]", t2));

        t2c.link(columns, udes);

        // Check correctness of dependencies
        t2c_deps = t2c.getDependencies();
        assertTrue( t2c_deps.contains( s.getColumn("T2", "A") ) );
        assertTrue( t2c_deps.contains( s.getColumn("T2", "B") ) );

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

        Column t1a = s.createColumn("A", "T", "Double");
        Column t1b = s.createColumn("B", "T", "String");

        // Add one record to link to
        t1.add();
        t1a.setValue(0,5.0); t1b.setValue(0,"bbb");

        //
        // Table 2 (referencing table to link records from)
        //
        Table t2 = s.createTable("T2");

        Column t2a = s.createColumn("A", "T2", "Double");
        Column t2b = s.createColumn("B", "T2", "String");

        Column t2c = s.createColumn("C", "T2", "T");

        // Add two records to link from
        t2.add();
        t2a.setValue(0,5.0); t2b.setValue(0,"bbb");
        t2.add();
        t2a.setValue(1,10.0); t2b.setValue(1,"ccc");

        return s;
    }


    @Test
    public void accuTest()
    {
        Schema s = this.createAccuSchema();
        Table t2 = s.getTable("T2");

        // Accu (group) formula
        Column ta = s.getColumn("T", "A");
        Column t2g = s.getColumn("T2", "G");

        // Create custom accu expression and bind to certain parameter paths
        UDE accuUde = new CustomAccuUde(Arrays.asList( new ColumnPath( s.getColumn("T2", "Id") ) ));

        ta.accumulate(new UdeExp4j("0.0", s.getTable("T")), accuUde, null, new ColumnPath(t2g));
        ta.evaluate(); // It has to also evaluate the accu (group) columns

        // Check correctness of dependencies
        List<Column> ta_deps = ta.getDependencies();
        assertTrue( ta_deps.contains( s.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( ta_deps.contains( s.getColumn("T2", "G") ) ); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // The same accu expression but translated from a formula
        accuUde = new UdeExp4j(" [out] + 2.0 * [Id] ", s.getTable("T2"));;

        ta.accumulate(new UdeExp4j("0.0", s.getTable("T")), accuUde, null, new ColumnPath(t2g));
        ta.evaluate();

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue( ta_deps.contains( s.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( ta_deps.contains( s.getColumn("T2", "G") ) ); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));


        // The same using formulas
        ta.accumulate(UDE.Exp4j, "0.0", " [out] + 2.0 * [Id] ", null, "T2", new NamePath("G"));
        ta.evaluate();

        // Check correctness of dependencies
        ta_deps = ta.getDependencies();
        assertTrue( ta_deps.contains( s.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( ta_deps.contains( s.getColumn("T2", "G") ) ); // Group path

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));

        t2.getColumn("Id").setValue(2, 5.0);
        s.evaluate(); // Both t2g and ta have to be evaluated
        assertEquals(30.0, ta.getValue(0));
    }

    Schema createAccuSchema() {
    	
        Schema schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("Id", "T", "Double");

        // Define accu column
        Column ta = schema.createColumn("A", "T", "Double");
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

        Column t2id = schema.createColumn("Id", "T2", "Double");

        // Define group column
        Column t2g = schema.createColumn("G", "T2", "T");
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




    @Test
    public void dependencyTest()
    {
    	// What do we need?
    	
    	// We want to add records asynchronously
    	// Each add increases the new interval of the table

    	// Each add result in column add and column status has to be updated
    	// User column becomes dirty and propagates deeply (its own dirty status will be resent immediately or by evaluate because it is a user column)
    	// Calc columns cannot be changed from outside but since it is a new record we do not lose anything so we can set the new value. But the column itself is marked dirty because this new value has to be computed from the formula.
    	// Link columns cannot be changed same as calc. And this column is also marked as dirty for future evaluation.
    	// Accu column also is marked dirty.
    	
    	// Evaluation means full evaluation (whole columns). But only dirty. 
    	// Evaluation starts from user columns which are marked clean without evaluation. 
    	// Then we evaluate next level as usual. And evaluated columns are marked clean.
    	// !!! Error columns are skipped and do not participate in evaluation as well as they are always dirty. 
    	// !!! Cycle columns are skipped and their status remains unchanged.
    	
    	// After evaluation, new table interval is merged with clean so we do not have new records anymore.
    	// We could add several records and then evaluate. 
    }

    @Test
    public void csvReadTest()
    {
        Schema schema = new Schema("My Schema");

        /*
        String path = "src/test/resources/example1/Order Details Status.csv"; // Relative to project directory

        Table table = schema.createFromCsv(path, true);
        
        assertEquals("Order Details Status", table.getName());
        assertEquals("Double", schema.getColumn("Order Details Status", "Status ID").getOutput().getName());
        assertEquals("String", schema.getColumn("Order Details Status", "Status Name").getOutput().getName());
        
        assertEquals(3L, schema.getColumn("Order Details Status", "Status ID").getValue(3));
        assertEquals("Shipped", schema.getColumn("Order Details Status", "Status Name").getValue(3));
        */
    }

    @Test
    public void classLoaderTest() 
    {
    	// Create class loader for the schema
    	// UDF class have to be always in nested folders corresponding to their package: either directly in file system or in jar
    	File classDir = new File("C:/TEMP/classes/");
        URL[] classUrl = new URL[1];
		try {
			classUrl[0] = classDir.toURI().toURL();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		URLClassLoader classLoader = new URLClassLoader(classUrl);
		// Now the schema is expected to dynamically load all class definitions for evaluators by using this class loader from this dir

		
    	Schema schema = new Schema("My Schema");

        Table table = schema.createTable("T");

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("A", "T", "Double");

        Column columnB = schema.createColumn("B", "T", "Double");
        //columnB.setDescriptor("{ \"class\":\"org.conceptoriented.sc.core.EvaluatorB\" }");
    }





    @Test
    public void evalexTest()
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

        //e = new com.udojava.evalex.Expression("MAX('aaa', 'bbb')");
        // We can define custom functions but they can take only numbers (as constants).
        // EvalEx does not have string parameters (literals).
        // It does not recognize quotes. So maybe simply introduce string literals even if they will be converted into numbers, that is, just like string in setVariable.
        // We need to change tokenizer by adding string literals in addition to numbers and then their processing.

        e.eval();
    }

}

class CustomCalcUde implements UDE {

    @Override public void setParamPaths(List<NamePath> paths) {}
    @Override public List<NamePath> getParamPaths() { return null; }

    List<ColumnPath> inputPaths = new ArrayList<>(); // The expression parameters are bound to these input column paths
    @Override public void setResolvedParamPaths(List<ColumnPath> paths) { this.inputPaths.addAll(paths); }
    @Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    @Override public void translate(String formula) {}
    @Override public List<BistroError> getTranslateErrors() { return null; }

    @Override public Object evaluate(Object[] params, Object out) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        return 2.0 * param + 1.0; // "2 * [A] + 1"
    }
    @Override public BistroError getEvaluateError() { return null; }

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

    @Override public void translate(String formula) {}
    @Override public List<BistroError> getTranslateErrors() { return null; }

    @Override public Object evaluate(Object[] params, Object out) {
        double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
        double outVal = out == null ? Double.NaN : ((Number)out).doubleValue();
        return outVal + 2.0 * param; // " [out] + 2.0 * [Id] "
    }
    @Override public BistroError getEvaluateError() { return null; }

    public CustomAccuUde() {
    }
    public CustomAccuUde(List<ColumnPath> inputPaths) {
        this.setResolvedParamPaths(inputPaths);
    }
}
