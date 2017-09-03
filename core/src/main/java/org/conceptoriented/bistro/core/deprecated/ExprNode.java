package org.conceptoriented.bistro.core.deprecated;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.core.expr.QNameBuilder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * It is a function definition with function name, function type and function formula. 
 * The formula can be a primitive expression or a tuple which is a combination of function formulas. 
 */
public class ExprNode {
	public static String OUT_VARIABLE_NAME = "out";
	
	public boolean isExp4j() { return true; }
	public boolean isEvalex() { return false; }
	
	//
	// Formula and its parameters
	//
	public String formula; // Function with column path always relative to the table

	public String tableName; // Table where we define a new (aggregated) column
	public Table table;

	public String pathName; // table-path-column-output (can be empty)
	public NamePath namePath;
	public List<Column> path;

	public String name; // Column name for which the result is computed (not necessarily starts from the table)
	public Column column;

	//
	// Tuple expression
	//
	public List<ExprNode> children = new ArrayList<ExprNode>(); // If the function is non-primitive, then its value is a combination

	public Record childrenToRecord() {
		Record r = new Record();
		children.forEach(x -> r.set(x.name, x.result));
		return r;
	}
	
	// Determine if it is a tuple syntactically (it seems to be intended to be a tuple), that is, something enclosed in curly brackets
	// In fact, it can be viewed as a syntactic check of validity and can be called isValidTuple
	public boolean isTuple() {
		if(this.formula == null) return false;

		int open = this.formula.indexOf("{");
		int close = this.formula.lastIndexOf("}");
		
		if(open < 0 || close < 0) return false;

		return true;
	}

	//
	// Status of the previous operation performed
	//
	public BistroError status;
	public ExprNode getErrorNode() { // Find first node with an error and return it. Otherwise, null
		if(this.status != null && this.status.code != BistroErrorCode.NONE) {
			return this;
		}
		for(ExprNode node : children) {
			ExprNode result = node.getErrorNode();
			if(result != null) return result;
		}
		return null;
	}

	protected List<PrimExprDependency> primExprDependencies = new ArrayList<PrimExprDependency>(); // Will be filled by parser and then augmented by binder
	
	public List<Column> getDependencies() { // Extract all unique column objects used (must be bound)
		List<Column> columns = new ArrayList<Column>();
		if(!this.isTuple()) {
			for(PrimExprDependency dep : this.primExprDependencies) {
				if(dep.columns == null) continue; // Probably not yet resolved
				dep.columns.forEach(x -> { if(!columns.contains(x)) columns.add(x); }); // Each dependency is a path and different paths can included same segments
			}
		}
		else {
			// Collect from all children
			for(ExprNode expr : children) {
				List<Column> childDeps = expr.getDependencies();
				if(childDeps == null) continue;
				childDeps.forEach(x -> { if(!columns.contains(x)) columns.add(x); } );
			}
			// Are member names also dependencies? Does this tuple column depend on its tuple member names (output table attributes)?
		}
		return columns;
	}

	//
	// Parse
	//

	/**
	 * Parse formulas by possibly building a tree of expressions with primitive expressions in the leaves.
	 * Any assignment has well defined structure NamePath=( {sequence of assignments} | expression)
	 * The result of parsing is a list of symbols.
	 */
	public void parse() {
		if(this.formula == null || this.formula.isEmpty()) return;

		this.primExprDependencies = new ArrayList<PrimExprDependency>();

		if(!this.isTuple()) { // Non-tuple (primitive)
			this.parsePrimExpr(this.formula);
		}
		else { // Tuple - combination of assignments
			this.parseTupleExpr(this.formula);
		}
	}

	// Find all occurrences of column paths in the primitive expression
	public void parsePrimExpr(String frml) {
		if(frml == null || frml.isEmpty()) return;
		
		//
		// Find all occurrences of individual columns names in square brackets
		//
		String ex =  "\\[(.*?)\\]";
		//String ex = "[\\[\\]]";
		Pattern p = Pattern.compile(ex,Pattern.DOTALL);
		Matcher matcher = p.matcher(frml);

		List<PrimExprDependency> names = new ArrayList<PrimExprDependency>();
		while(matcher.find())
		{
			int s = matcher.start();
			int e = matcher.end();
			String name = matcher.group();
			PrimExprDependency entry = new PrimExprDependency();
			entry.start = s;
			entry.end = e;
			names.add(entry);
		}
		
		//
		// Create paths by concatenating dot separated name sequences
		//
		for(int i = 0; i < names.size(); i++) {
			if(i == names.size()-1) { // Last element does not have continuation
				this.primExprDependencies.add(names.get(i));
				break;
			}
			
			int thisEnd = names.get(i).end;
			int nextStart = names.get(i+1).start;
			
			if(frml.substring(thisEnd, nextStart).trim().equals(".")) { // There is continuation.
				names.get(i+1).start = names.get(i).start; // Attach this name to the next name as a prefix
			}
			else { // No continuation. Ready to copy as path.
				this.primExprDependencies.add(names.get(i));
			}
		}

    	//
		// Process the paths
		//
		QNameBuilder parser = new QNameBuilder();

		for(PrimExprDependency dep : this.primExprDependencies) {
			dep.pathName = frml.substring(dep.start, dep.end);
			dep.qname = parser.buildQName(dep.pathName); // TODO: There might be errors here, e.g., wrong characters in names
		}
		
		//
		// Parse accu path which is treated as a normal access path along with those in the formula
		//
		if(this.pathName != null && !this.pathName.trim().isEmpty()) {

			this.namePath = parser.buildQName(this.pathName);
			if(this.namePath == null || this.namePath.names.size() == 0) {
				this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Syntax error in group path.", "Error group path: '" + this.pathName + "'");
				return;
			}
			
			// Add group path to dependencies (it must be computed before accumulation)
			PrimExprDependency pathDep = new PrimExprDependency();
			pathDep.paramName = OUT_VARIABLE_NAME;
			pathDep.pathName = this.pathName;
			pathDep.qname = this.namePath;
			pathDep.start = -1; // Means that it is not in the formula
			pathDep.end = -1;
			this.primExprDependencies.add(pathDep);
		}

		this.status = new BistroError(BistroErrorCode.NONE, "Parsed successfully.", "");
	}
	
	public void parseTupleExpr(String frml) {
		
		//
		// Check correct enclosure (curly brackets)
		//
		int open = frml.indexOf("{");
		int close = frml.lastIndexOf("}");

		if(open < 0 || close < 0 || open >= close) {
			this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Problem with curly braces.", "Tuple expression is a list of assignments in curly braces.");
			return;
		}

		String sequence = frml.substring(open+1, close).trim();

		//
		// Build a list of members from comma separated list
		//
		List<String> members = new ArrayList<String>();
		int previousSeparator = -1;
		int level = 0; // Work only on level 0
		for(int i=0; i<sequence.length(); i++) {
			if(sequence.charAt(i) == '{') {
				level++;
			}
			else if(sequence.charAt(i) == '}') {
				level--;
			}
			
			if(level > 0) { // We are in a nested block. More closing parentheses are expected to exit from this block.
				continue;
			}
			else if(level < 0) {
				this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Problem with curly braces.", "Opening and closing curly braces must match each other.");
				return;
			}
			
			// Check if it is a member separator
			if(sequence.charAt(i) == ';') {
				members.add(sequence.substring(previousSeparator+1, i));
				previousSeparator = i;
			}
		}
		members.add(sequence.substring(previousSeparator+1, sequence.length()));

		//
		// Create child tuples from members and parse them
		//
		for(String member : members) {
			int eq = member.indexOf("=");
			if(eq < 0) {
				this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "No equality sign.", "Tuple expression is a list of assignments using equality sign.");
				return;
			}
			String left = member.substring(0, eq).trim();
			if(left.startsWith("[")) left = left.substring(1);
			if(left.endsWith("]")) left = left.substring(0,left.length()-1);
			String rigth = member.substring(eq+1).trim();

			ExprNode childTuple = new ExprNode();

			childTuple.formula = rigth;
			childTuple.tableName = this.tableName; // All child right expressions are relative to the same parent table
			childTuple.pathName = "";
			childTuple.name = left; // Relative to the output table

			childTuple.parse();

			if(childTuple.status != null && childTuple.status.code != BistroErrorCode.NONE) { // Propagate child error to the parent
				this.status = childTuple.status;
				return;
			}

			this.children.add(childTuple);
		}

		this.status = new BistroError(BistroErrorCode.NONE, "Parsed successfully.", "");
	}

	//
	// Bind
	//

	// Resolve all symbols found after parsing and store them in dependency graph or in this expression fields
	public void bind() {
		
		if(!this.isTuple()) { // Non-tuple: either calculated or aggregated

			this.bindPrimExpr(); // Resolve symbols used in the formula relative to the main table

			// Build the final (native) expression
			if(this.isExp4j()) {
				this.exp4jExpression = this.buildExp4jExpression();
			}
			else if(this.isEvalex()) {
				this.evalexExpression = this.buildEvalexExpression();
			}
		}
		else { // Tuple - combination of assignments
			Table output = this.column.getOutput();

			for(ExprNode expr : children) {
				Column col = output.getSchema().getColumn(output.getName(), expr.name); // Really resolve name as a column in our type table
				if(col != null) {
					expr.column = col;
				}
				else {
					this.status = new BistroError(BistroErrorCode.BIND_ERROR, "Column name not found.", "Error finding column with the name [" + expr.name + "]");
					return;
				}
				
				expr.table = this.table; // Same for all children
				expr.bind(); // Recursion

				if(expr.status != null && expr.status.code != BistroErrorCode.NONE) {
					this.status = expr.status;
					return;
				}
			}

			this.status = new BistroError(BistroErrorCode.NONE, "Resolved successfully.", "");
		}
	}

	public void bindPrimExpr() {

		if(this.primExprDependencies == null) { // Here we store all symbols in the formula that need to be resolved
			return;
		}

		Schema schema = this.column.getSchema(); // Resolve against this schema

		//
		// Resolve table
		//
		this.table = schema.getTable(this.tableName); // Resolve table name
		
		if(this.table == null) {
			this.status = new BistroError(BistroErrorCode.BIND_ERROR, "Cannot resolve table.", "Error resolving table " + this.tableName);
			return;
		}
		
		//
		// Resolve each column path in the formula relative to the table. Group path is also in this list.
		//
		for(PrimExprDependency dep : this.primExprDependencies) {

			dep.columns = dep.qname.resolveColumns(this.table); // Try to really resolve symbol

			if(dep.columns == null || dep.columns.size() < dep.qname.names.size()) {
				this.status = new BistroError(BistroErrorCode.BIND_ERROR, "Cannot resolve columns.", "Error resolving columns " + dep.pathName);
				return;
			}
		}

		//
		// Resolve accu path (if any) relative to the table
		//
		this.path = null;
		if(this.namePath != null) {

			this.path = this.namePath.resolveColumns(this.table); // Try to really resolve symbol

			if(this.path == null || this.path.size() < this.namePath.names.size()) {
				this.status = new BistroError(BistroErrorCode.BIND_ERROR, "Cannot resolve columns.", "Error resolving column " + this.pathName);
				return;
			}

			// Check the sequence: table-path-column. End of last path segment is start of column. 
			if(this.path.get(this.path.size()-1).getOutput() != this.column.getInput()) {
				this.status = new BistroError(BistroErrorCode.BIND_ERROR, "Wrong group path.", "Group path type has to be equal to the table where accumulation column is defined: " + this.pathName);
				return;
			}
		}

	}
	
	// Replace all occurrences of column paths in the formula by variable names from the symbol table
	private String transformFormula() {
		StringBuffer buf = new StringBuffer(this.formula);
		for(int i = this.primExprDependencies.size()-1; i >= 0; i--) {
			PrimExprDependency dep = this.primExprDependencies.get(i);
			if(dep.start < 0 || dep.end < 0) continue; // Some dependencies are not from formula (e.g., group path)
			dep.paramName = "__p__"+i;
			buf.replace(dep.start, dep.end, dep.paramName);
		}
		return buf.toString();
	}

	//
	// Evaluate
	//

	// Expression that is used during evaluation
	protected net.objecthunter.exp4j.Expression exp4jExpression;
	protected com.udojava.evalex.Expression evalexExpression = null;

	// Build exp4j expression
	protected net.objecthunter.exp4j.Expression buildExp4jExpression() {

		String transformedFormula = this.transformFormula();

		//
		// Create a list of variables used in the expression
		//
		Set<String> vars = new HashSet<String>();
		Map<String, Double> vals = new HashMap<String, Double>();
		for(PrimExprDependency dep : this.primExprDependencies) {
			if(dep.paramName == null || dep.paramName.trim().isEmpty()) continue;
			vars.add(dep.paramName);
			vals.put(dep.paramName, 0.0);
		}
		// Set<String> vars = this.primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		
		// Add the current output value as a special (reserved) variable
		if(!vars.contains(OUT_VARIABLE_NAME)) vars.add(OUT_VARIABLE_NAME);
		vals.put(OUT_VARIABLE_NAME, 0.0);

		//
		// Create expression object with the transformed formula
		//
		net.objecthunter.exp4j.Expression exp = null;
		try {
			net.objecthunter.exp4j.ExpressionBuilder builder = new net.objecthunter.exp4j.ExpressionBuilder(transformedFormula);
			builder.variables(vars);
			exp = builder.build(); // Here we get parsing exceptions which might need be caught and processed
		}
		catch(Exception e) {
			this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			return null;
		}

		//
		// Validate
		//
		exp.setVariables(vals); // Validation requires variables to be set
		net.objecthunter.exp4j.ValidationResult res = exp.validate(); // Boolean argument can be used to ignore unknown variables
		if(!res.isValid()) {
			this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Expression error.", res.getErrors() != null && res.getErrors().size() > 0 ? res.getErrors().get(0) : "");
			return null;
		}

		return exp;
	}

	// Build Evalex expression
	protected com.udojava.evalex.Expression buildEvalexExpression() {

		String transformedFormula = this.transformFormula();

		//
		// Create a list of variables used in the expression
		//
		Set<String> vars = new HashSet<String>();
		Map<String, Double> vals = new HashMap<String, Double>();
		for(PrimExprDependency dep : this.primExprDependencies) {
			if(dep.paramName == null || dep.paramName.trim().isEmpty()) continue;
			vars.add(dep.paramName);
			vals.put(dep.paramName, 0.0);
		}
		// Set<String> vars = this.primExprDependencies.stream().map(x -> x.paramName).collect(Collectors.toCollection(HashSet::new));
		
		// Add the current output value as a special (reserved) variable
		if(!vars.contains(OUT_VARIABLE_NAME)) vars.add(OUT_VARIABLE_NAME);
		vals.put(OUT_VARIABLE_NAME, 0.0);

		//
		// Create expression object with the transformed formula
		//
		final com.udojava.evalex.Expression exp;
		try {
			exp = new com.udojava.evalex.Expression(transformedFormula);
		}
		catch(Exception e) {
			this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Expression error.", e.getMessage());
			return null;
		}

		//
		// Validate
		//
		vars.forEach(x -> exp.setVariable(x, new BigDecimal(1.0)));
    	try {
    		exp.toRPN(); // Generates prefixed representation but can be used to check errors (variables have to be set in order to correctly determine parse errors)
    	}
    	catch(com.udojava.evalex.Expression.ExpressionException ee) {
			this.status = new BistroError(BistroErrorCode.PARSE_ERROR, "Expression error.", ee.getMessage());
			return null;
    	}

		return exp;
	}

	public Object result; // Result of evaluation: either primitive value or record id

	protected void setFormulaExpressionVariables(long i) { // Pass all variable values for the specified input to the compute expression

		for(PrimExprDependency dep : this.primExprDependencies) {
			if(dep.paramName == null || dep.paramName.trim().isEmpty()) continue;
			Object value = dep.columns.get(0).getData().getValue(dep.columns, i); // Read column value
			if(value == null) value = Double.NaN;
			try {
				if(this.isExp4j()) {
					this.exp4jExpression.setVariable(dep.paramName, ((Number)value).doubleValue());
				}
				else if(this.isEvalex()) {
					
				}
			}
			catch(Exception e) {
				;
			}
		}
		
		// Data
		ColumnData data = this.column.getData();

		// Set current output value as a special variable
		Object outputValue;
		if(this.path == null) {
			outputValue = data.getValue(i);
		}
		else {
			long g = (Long) this.path.get(0).getData().getValue(this.path, i); // Find group element
			outputValue = null;
			if(g >= 0) outputValue = data.getValue(g);
		}
		if(outputValue == null) outputValue = Double.NaN;
		try {
			if(this.isExp4j()) {
				this.exp4jExpression.setVariable(OUT_VARIABLE_NAME, ((Number)outputValue).doubleValue());
			}
			else if(this.isEvalex()) {
				
			}
		}
		catch(Exception e) {
			;
		}
	}

	public void evaluate(long i) {
		
		if(!this.isTuple()) { // Primitive expression
			
			// Determine if the formula can be and has to be evaluated
			boolean numericEvalution = true;
			if(this.primExprDependencies.size() == 1) {
				PrimExprDependency dep = this.primExprDependencies.get(0);
				
				// Check if param name is equal to the whole formula (there are no operations)
				// Alternatively, check if expression tree has one node with parameter
				if(this.formula.trim().equals(dep.pathName.trim())) {
					numericEvalution = false;
				}
			}
			
			if(!numericEvalution) { // No evaluation needed or possible for this formula
				// Copy the value to the output (since no operations)
				PrimExprDependency dep = this.primExprDependencies.get(0);
				Object value = dep.columns.get(0).getData().getValue(dep.columns, i); // Read column value
				result = value;
			}
			else { // Arithmetic expression that needs to be evaluated
				
				this.setFormulaExpressionVariables(i); // For each input, read all necessary column values from fact table and the current output from the group table

				// Build the final (native) expression
				if(this.isExp4j()) {
					result = this.exp4jExpression.evaluate();
				}
				else if(this.isEvalex()) {
					result = this.evalexExpression.eval();
				}

			}
		}
		else { // Tuple
			
			for(ExprNode expr : children) { // Evaluation recursion down to primitive expressions which compute primitive values
				expr.evaluate(i); // Recursion
			}
			// After recursion, members are supposed to store result values
			
			// Combine child results into a tuple and find it in the output table
			Table output = this.column.getOutput();
			Record r = this.childrenToRecord(); // Output value is this record
			long row = output.find(r, true); // But we store a record reference so find it
			result = row; // We store id of the record - not the record itself
		}
	}
	
	public ExprNode() {
	}
}

class PrimExprDependency {
	public int start;
	public int end;
	public String pathName;
	public String paramName;
	public NamePath qname;
	public List<Column> columns;
}
