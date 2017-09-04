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
import org.apache.commons.lang3.tuple.Pair;
import org.conceptoriented.bistro.core.expr.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class Tests {

    @BeforeClass
    public static void setUpClass() {
    }

    Schema schema;

    @Before
    public void setUp() {
    }


    // Calculate-Accumulate-Link (map-reduce-join)
    // Scenario
    // - SCHEMA. Create schema, tables and columns:
    //   - names - by default unique names, if not unique then exception/error creating -> see how elements are created in other frameworks
    //   - data type/operation/definition - how to specify data type: enum, string, guid. Data type is *native* storage and native represenation/operations.
    //   - error handling - exceptions/returns - result for operations
    //   - status handling - fields maybe referencing previous error/exception
    //   - column kind - by default USER (not derived, manually set values will be overwritten, no evaluation/transaltion)
    //   - table kind - by default derived - manually added/removed records will be deleted/overwritten
    // - DEFINE columns. Set column kind and evaluator/formula/expression/lambda
    //   - setCalc/setAccu/setLink/setUser - kind is changed by setting the evaluator (not separately)
    //   - Objects (Lambdas, Expressions, Tables/Columns)
    //     - (EvaluatorCalc/Accu/Link) - directly provide evaluator
    //     - Expressions (instances of custom classes implementing UDE) and other objects used to instantiate an evaluator
    //     - Lambdas and other objects used to instantiate an evaluator (directly or by creating expression instances)
    //   - Syntactic:
    //     - Definition object used to create an evaluator by translating its formulas and strings into the necessary objects used to create an evaluator
    //       - We can instantiate and pass the definition object and the system will use its public API to translate
    //       - We can instantiate definition, do translation and pass the objects
    //     - Formulas and strings used to create an evaluator (by translating them to the necessary objects)
    //       - We can pass the strings and the system will do everything
    //       - We can use the strings to create definition or to create the objects directly (expressions, tables, columns etc.)
    // - TRANSLATE
    //   - it is optional and useful only for checking validity, e.g., for UI, but not for batch
    //   - if we use evaluate method then translation will be anyway performed and errors generated
    //   - translate formulas if any into expressions
    //   - use (possibly translated) expressions to created evaluators
    //   - use (if provided) lambdas to created expressions/evaluators
    //   - set evaluators (if not set in the case of direct assignment)
    //   - check dependencies and propagation of errors
    // - EVALUATE
    //   - translate
    //   - build sequence of evaluation using dependency graph
    //   - evaluator sequentially by checking evaluate errors

    @Test
    public void exampleOneTest() { // Schema operations
    }

    @Test
    public void tableDataTest() { // Manual operations table id ranges
        // Population: add, delete id to/from table
        //   Problem: we cannot do it for columns - ColumnData should not support add/delete but maybe only update id range. So ColumnData is essentially immutable and can be resampled only from thier input Table.
        //   Question: how columns store their range of inputs if it has to be shared? Option 1: use input table range. Option 2: each column has its range.
        //   Problem: we do not want to delete intermediate values (maybe only mark them as deleted).
        //     Solution 1: We do not have add and delete - we have extend range (add) and shrink range (delete).
        //     Solution 2: In future, allow for individual deletes and maybe inserts.

        // Get result range from table. Check for validity after operations.
        // Note: do not use records - they are treated as setting multiple columns simultaniously - not for add/delete
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
    public void columnDataTest() { // Manual operations table id ranges
        // Prepopulate table for experiments with column data
        Schema s = new Schema("My Schema");
        Table t = s.createTable("My Table");
        Column c1 = s.createColumn("My Table", "My Column", "Double"); // <-- Should not we objects? Name use should be limited, indeed.

        long id = t.add();
        assertEquals(0, id);
        long id2 = t.add();
        c1.setValue(id2, 1.0);
        assertEquals(1.0, (double)c1.getValue(id2), Double.MIN_VALUE);

        Column c2 = s.createColumn("My Table", "My Column 2", "String");
        long id3 = t.add();
        c2.setValue(id3, "StringValue");
        assertEquals("StringValue", (String)c2.getValue(id3));

        assertEquals(0, t.getIdRange().start);
        assertEquals(3, t.getIdRange().end);

        // Records.
        // Working with multiple columns (records)
        long id4 = t.add();
        List<Column> cols = new ArrayList<>(Arrays.asList(c1,c2));
        List<Object> vals = new ArrayList<>(Arrays.asList(2.0,"StringValue 2"));
        t.setValues(id4, cols, vals);
        assertEquals(2.0, (double)c1.getValue(id4), Double.MIN_VALUE);
        assertEquals("StringValue 2", (String)c2.getValue(id4));

        // Record search

        // TODO: Wrong Record/Map: check correct exception

        // TODO: Wrong ids: check exception for wrong ids

        // Data type conversion while setting/getting: use different types and formats and options.
        // Check right conversion or right exceptions. Rely on our UtilsData for conversion and specify/document what has to happen.
        // Working with NULLs, NaN etc.

        // Change column type (leads to ColumnData migration with all the data in the case of USER)
    }

    @Test
    public void exampleThreeTest() { // Manual operations with table data (records). Develop convenient record object and operations (maybe with JSON).

    }

    @Test
    public void exampleFourTest() { // Calc columns: define in different ways

    }




    @Test
    public void schemaTest()
    {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");
        Table table = schema.createTable("T");

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("T", "A", "Double");

        // Calculated column. It has a user-defined evaluation method (plug-in, mapping, coel etc.)
        // This column can read its own and other column values, and it knows about new/valid/old record ranges 
        // It is expected to write/update its own value
        // If necessary, it can update its type/output by pushing records to its type/output table and using the returned row id for writing into itself
        Column columnB = schema.createColumn("T", "B", "Double");
        //String descr = "{ `class`:`org.conceptoriented.sc.core.EvaluatorB`, `dependencies`:[`A`] }";
        //columnB.setDescriptor(descr.replace('`', '"'));

        // Add one or more records to the table
        Record record = new Record();

        record.set("A", 5.0);
        Record.addToTable(table, record);

        record.set("A", 10.0);
        Record.addToTable(table, record);

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        record.set("A", 20.0);
        Record.addToTable(table, record);

        // Evaluate schema by updating its schema. Mark new records as clean and finally remove records for deletion.
        schema.evaluate();
        
        // Check the result

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

    	e.getExpressionTokenizer(); // Does not detect errors
    	
    	e.setVariable("a", "2.4"); // Can work with strings (representing numbers)
    	e.setVariable("b", new BigDecimal(9.253));

    	// Validate
    	try {
        	e.toRPN(); // Generates prefixed representation but can be used to check errors (variables have to be set in order to correctly determine parse errors)
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

    @Test
    public void calcFormulaTest() 
    {
    	Schema schema = createCalcSchema();
        Column columnA = schema.getColumn("T", "A");
        Column columnB = schema.getColumn("T", "B");

        columnB.setDefinitionCalc(new ColumnDefinitionCalc("2 * [A] + 1", ExpressionKind.EXP4J));

        columnB.translate();
        
        // Check correctness of dependencies
        List<Column> depsB = columnB.getDependencies();
        assertTrue( depsB.contains(columnA) );

        columnB.evaluate();

        assertEquals(11.0, (Double)columnB.getValue(0), 0.00001);
        assertEquals(Double.NaN, columnB.getValue(1));
        assertEquals(13.0, (Double)columnB.getValue(2), 0.00001);
    }

    @Test
    public void calcUdeTest() // Test custom class for calc column 
    {
    	Schema schema = createCalcSchema();
        Column columnA = schema.getColumn("T", "A");
        Column columnB = schema.getColumn("T", "B");
        
        // Create ColumnEvaluatorCalc by using a custom Java class as UserDefinedExpression
        List<ColumnPath> inputPaths = Arrays.asList( new ColumnPath(Arrays.asList(columnA)) ); // Bind to column objects directly (without names)
        UDE ude = new CustomCalcUde(inputPaths);
        ColumnEvaluatorCalc eval = new ColumnEvaluatorCalc(columnB, ude);
        columnB.setEvaluatorCalc(eval);

        columnB.translate(); // Only to extract dependencies

        // Check correctness of dependencies
        List<Column> depsB = columnB.getDependencies();
        assertTrue( depsB.contains(columnA) );

        columnB.evaluate();

        assertEquals(11.0, (Double)columnB.getValue(0), 0.00001);
        assertEquals(Double.NaN, columnB.getValue(1));
        assertEquals(13.0, (Double)columnB.getValue(2), 0.00001);
    }
    protected Schema createCalcSchema() {
    	schema = new Schema("My Schema");
        Table table = schema.createTable("T");
        Column columnA = schema.createColumn("T", "A", "Double");
        Column columnB = schema.createColumn("T", "B", "Double");
        columnB.setKind(ColumnKind.CALC);
        
        Record record = new Record();
        record.set("A", 5.0);
        Record.addToTable(table, record);
        record.set("A", null);
        Record.addToTable(table, record);
        record.set("A", 6);
        Record.addToTable(table, record);

        return schema;
    }
    class CustomCalcUde implements UDE {
    	
    	@Override public void setParamPaths(List<NamePath> paths) {}
    	@Override public List<NamePath> getParamPaths() { return null; }

    	List<ColumnPath> inputPaths = new ArrayList<ColumnPath>(); // The expression parameters are bound to these input column paths
    	@Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    	@Override public void translate(String formula) {}
    	@Override public List<BistroError> getTranslateErrors() { return null; }

    	@Override public Object evaluate(Object[] params, Object out) { 
    		double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
    		return 2.0 * param + 1.0; // "2 * [A] + 1" 
		}
    	@Override public BistroError getEvaluateError() { return null; }
    	
    	public CustomCalcUde(List<ColumnPath> inputPaths) {
    		this.inputPaths.addAll(inputPaths);
    	}
    }



    @Test
    public void linkFormulaTest()
    {
    	Schema schema = createLinkSchema();

        Column c5 = schema.getColumn("T2", "C");

    	c5.setDefinitionLink(new ColumnDefinitionLink(" { [A] = [A]; [B] = [B] } ", ExpressionKind.EXP4J));

        c5.translate();

        // Check correctness of dependencies
        List<Column> depsC5 = c5.getDependencies();
        assertTrue( depsC5.contains( schema.getColumn("T2", "A") ) );
        assertTrue( depsC5.contains( schema.getColumn("T2", "B") ) );

        c5.evaluate();

        assertEquals(0L, c5.getValue(0));
        assertEquals(1L, c5.getValue(1));
    }
    
    @Test
    public void linkUdeTest()
    {
    	Schema schema = createLinkSchema();

        Column c5 = schema.getColumn("T2", "C");

        // Define evaluator for this formula: " { [A] = [A]; [B] = [B] } "
        Column c1 = schema.getColumn("T", "A");
        Column c2 = schema.getColumn("T", "B");
        UDE expr1 = new UdeJava("[A]", c1.getInput());
        UDE expr2 = new UdeJava("[B]", c2.getInput());

        List<Pair<Column,UDE>> udes = new ArrayList<Pair<Column,UDE>>();
        udes.add(Pair.of(schema.getColumn("T2", "A"), expr1));
        udes.add(Pair.of(schema.getColumn("T2", "B"), expr2));
        
        ColumnEvaluatorLink eval = new ColumnEvaluatorLink(c5, udes);
        c5.setEvaluatorLink(eval);

        c5.translate();

        // Check correctness of dependencies
        List<Column> depsC5 = c5.getDependencies();
        assertTrue( depsC5.contains( schema.getColumn("T2", "A") ) );
        assertTrue( depsC5.contains( schema.getColumn("T2", "B") ) );

        c5.evaluate();

        assertEquals(0L, c5.getValue(0));
        assertEquals(1L, c5.getValue(1));
    }
    Schema createLinkSchema() {
    	// Create and configure: schema, tables, columns
        schema = new Schema("My Schema");

        //
        // Table 1 (type table)
        //
        Table t1 = schema.createTable("T");

        Column c1 = schema.createColumn("T", "A", "Double");
        Column c2 = schema.createColumn("T", "B", "String");

        // Add one or more records to the table
        Record.addToTable(t1, Record.fromJson("{ A: 5.0, B: \"bbb\" }"));

        //
        // Table 2
        //
        Table t2 = schema.createTable("T2");

        Column c3 = schema.createColumn("T2", "A", "Double");
        Column c4 = schema.createColumn("T2", "B", "String");

        Column c5 = schema.createColumn("T2", "C", "T");
        c5.setKind(ColumnKind.LINK);

        // Add one or more records to the table
        Record.addToTable(t2, Record.fromJson("{ A: 5.0, B: \"bbb\" }"));
        Record.addToTable(t2, Record.fromJson("{ A: 10.0, B: \"ccc\" }"));

        return schema;
    }



    @Test
    public void accuFormulaTest()
    {
        schema = this.createAccuSchema();

        // Link (group) formula
        Column t2g = schema.getColumn("T2", "G");
        t2g.setDefinitionLink(new ColumnDefinitionLink(" { [Id] = [Id] } ", ExpressionKind.EXP4J));
        
        // Accu formula
        Column ta = schema.getColumn("T", "A");
        ta.setDefinitionAccu(new ColumnDefinitionAccu("", " [out] + 2.0 * [Id] ", null, "T2", "[G]", ExpressionKind.EXP4J));

        //
        // Translate and evaluate
        //
        schema.translate();

        // Check correctness of dependencies
        List<Column> depsTa = ta.getDependencies();
        assertTrue( depsTa.contains( schema.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( depsTa.contains( schema.getColumn("T2", "G") ) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }
    @Test
    public void accuUdeTest()
    {
        schema = this.createAccuSchema();

        // Link (group) formula
        Column t2g = schema.getColumn("T2", "G");
        t2g.setDefinitionLink(new ColumnDefinitionLink(" { [Id] = [Id] } ", ExpressionKind.EXP4J));
        
        // Accu evaluator
        Column ta = schema.getColumn("T", "A");
        
        UDE initUde = new UdeJava("0.0", schema.getTable("T"));

        //UserDefinedExpression accuUde = new UdeJava(" [out] + 2.0 * [Id] ", schema.getTable("T2"));;
        List<ColumnPath> inputPaths = Arrays.asList( new ColumnPath( schema.getColumn("T2", "Id") ) );
        UDE accuUde = new CustomAccuUde(inputPaths);

        ColumnPath accuPathColumns = new ColumnPath(schema.getColumn("T2", "G"));
        
        ColumnEvaluatorAccu eval = new ColumnEvaluatorAccu(ta, initUde, accuUde, null, accuPathColumns);
        ta.setEvaluatorAccu(eval);
        
        //
        // Translate and evaluate
        //
        schema.translate();

        // Check correctness of dependencies
        List<Column> depsTa = ta.getDependencies();
        assertTrue( depsTa.contains( schema.getColumn("T2", "Id") ) ); // Used in formula
        assertTrue( depsTa.contains( schema.getColumn("T2", "G") ) ); // Group path

        schema.evaluate();

        assertEquals(20.0, ta.getValue(0));
        assertEquals(20.0, ta.getValue(1));
        assertEquals(0.0, ta.getValue(2));
    }
    protected Schema createAccuSchema() {
    	
        schema = new Schema("My Schema");

        //
        // Table 1 (group table)
        //
        Table t1 = schema.createTable("T");

        Column tid = schema.createColumn("T", "Id", "Double");

        // Define accu column
        Column ta = schema.createColumn("T", "A", "Double");
        ta.setKind(ColumnKind.ACCU);

        Record.addToTable(t1, Record.fromJson("{ Id: 5.0 }"));
        Record.addToTable(t1, Record.fromJson("{ Id: 10.0 }"));
        Record.addToTable(t1, Record.fromJson("{ Id: 15.0 }"));

        //
        // Table 2 (fact table)
        //
        Table t2 = schema.createTable("T2");

        Column t2id = schema.createColumn("T2", "Id", "Double");

        // Define group column
        Column t2g = schema.createColumn("T2", "G", "T");
        t2g.setKind(ColumnKind.LINK);

        Record.addToTable(t2, Record.fromJson("{ Id: 5.0 }"));
        Record.addToTable(t2, Record.fromJson("{ Id: 5.0 }"));
        Record.addToTable(t2, Record.fromJson("{ Id: 10.0 }"));
        Record.addToTable(t2, Record.fromJson("{ Id: 20.0 }"));

        return schema;
    }
    class CustomAccuUde implements UDE {
    	
    	@Override public void setParamPaths(List<NamePath> paths) {}
    	@Override public List<NamePath> getParamPaths() { return null; }

    	List<ColumnPath> inputPaths = new ArrayList<ColumnPath>(); // The expression parameters are bound to these input column paths
    	@Override public List<ColumnPath> getResolvedParamPaths() { return inputPaths; }

    	@Override public void translate(String formula) {}
    	@Override public List<BistroError> getTranslateErrors() { return null; }

    	@Override public Object evaluate(Object[] params, Object out) {
    		double param = params[0] == null ? Double.NaN : ((Number)params[0]).doubleValue();
    		double outVal = out == null ? Double.NaN : ((Number)out).doubleValue();
    		return outVal + 2.0 * param; // " [out] + 2.0 * [Id] " 
		}
    	@Override public BistroError getEvaluateError() { return null; }
    	
    	public CustomAccuUde(List<ColumnPath> inputPaths) {
    		this.inputPaths.addAll(inputPaths);
    	}
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
        schema = new Schema("My Schema");

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

		
    	schema = new Schema("My Schema");

        Table table = schema.createTable("T");

        // Data column will get its data from pushed records (input column)
        Column columnA = schema.createColumn("T", "A", "Double");

        Column columnB = schema.createColumn("T", "B", "Double");
        //columnB.setDescriptor("{ \"class\":\"org.conceptoriented.sc.core.EvaluatorB\" }");
    }

}
