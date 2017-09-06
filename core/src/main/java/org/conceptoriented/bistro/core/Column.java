package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.ColumnDefinitionAccu;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionCalc;
import org.conceptoriented.bistro.core.expr.ColumnDefinitionLink;
import org.conceptoriented.bistro.core.expr.ExpressionKind;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class Column {
	private Schema schema;
	public Schema getSchema() {
		return this.schema;
	}
	
	private final UUID id;
	public UUID getId() {
		return this.id;
	}

	private String name;
	public String getName() {
		return this.name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	private Table input;
	public Table getInput() {
		return this.input;
	}
	public void setInput(Table table) {
		this.input = table;
	}

	private Table output;
	public Table getOutput() {
		return this.output;
	}
	public void setOutput(Table table) { this.output = table; this.setValue(null); }

	private boolean key = false;
	public boolean isKey() {  return this.key; }

    //
    // Data dirty state: added/removed inputs (set population), and change outputs (function updated)
    //

    protected boolean isChanged = false;

    //
	// Data (public)
	//

    private ColumnData data;

    public Object getValue(long id) { return this.data.getValue(id); }

    public void setValue(long id, Object value) { this.data.setValue(id, value); this.isChanged = true; }

    public void setValue(Object value) { this.data.setValue(value); this.isChanged = true; }

    //
    // Data (protected). These are used from Table only (all columns change their ranges simultaniously) and from users - users add/remove elements via tables.
    //

    protected void add() { this.data.add(); this.isChanged = true; }
	protected void add(long count) { this.data.add(count); this.isChanged = true; }

    protected void remove() { this.data.remove(1); this.isChanged = true; }
    protected void remove(long count) { this.data.remove(count); this.isChanged = true; }



    @Deprecated // This probably should also moved to UtilsData which knows about all data types, conversions, default values etc.
	public Object getDefaultValue() { // TODO: Depends on the column type. Maybe move to Data class or to Evaluator/Definition
		Object defaultValue;
		if(this.getOutput().isPrimitive()) {
			defaultValue = 0.0;
		}
		else {
			defaultValue = null;
		}
		return defaultValue;
	}

    @Deprecated // Convenience method. Should be part of UtilsData with casts/conversions. Replace it everywhere by add() and setValue()
	public long appendValue(Object value) {
        // Cast the value to type of this column
        if(this.getOutput().getName().equalsIgnoreCase("String")) {
            try { value = value.toString(); }
            catch (Exception e) { value = ""; }
        }
        else if(this.getOutput().getName().equalsIgnoreCase("Double") || this.getOutput().getName().equalsIgnoreCase("Integer")) {
            if(value instanceof String) {
                try { value = NumberFormat.getInstance(Locale.US).parse(((String)value).trim()); }
                catch (ParseException e) { value = Double.NaN; }
            }
        }

        this.data.add(); // TODO: it does not work. we need to get largest (new) id but we can do it only from the table, so remove this method
        this.data.setValue(0, value);
        return 0;
    }

    //
	// Formula kind
	//
	
	protected ColumnKind kind;
	public ColumnKind getKind() {
		return this.kind;
	}
	public void setKind(ColumnKind kind) {
		this.kind = kind;
	}
	public boolean isDerived() {
		if(this.kind == ColumnKind.CALC || this.kind == ColumnKind.ACCU || this.kind == ColumnKind.LINK) {
			return true;
		}
		return false;
	}

	//
	// Three formula types
	//
/* We do not have formulas - they could be represented in extensions or associated objects if necessary - column is only for computations using objects that are able to compute
	// It is used for all definition types (by default) but every definition has its own expression kind
	public ExpressionKind expressionKind = ExpressionKind.EXP4J;

	// Calc formula
	protected ColumnDefinitionCalc definitionCalc;
	public ColumnDefinitionCalc getDefinitionCalc() {
		return this.definitionCalc;
	}
	public void setDefinitionCalc(ColumnDefinitionCalc definition) {
		this.definitionCalc = definition;
		this.setFormulaChange(true);
	}

	// Link formula
	protected ColumnDefinitionLink definitionLink;
	public ColumnDefinitionLink getDefinitionLink() {
		return this.definitionLink;
	}
	public void setDefinitionLink(ColumnDefinitionLink definition) {
		this.definitionLink = definition;
		this.setFormulaChange(true);
	}

	//
	// Accumulation formula
	//
	protected ColumnDefinitionAccu definitionAccu;
	public ColumnDefinitionAccu getDefinitionAccu() {
		return this.definitionAccu;
	}
	public void setDefinitionAccu(ColumnDefinitionAccu definition) {
		this.definitionAccu = definition;
		this.setFormulaChange(true);
	}
*/

	//
	// Formula dirty status (own or inherited)
	//

	/**
	 * Status of the data defined by this (and only this) column formula: clean (up-to-date) or dirty.
	 * This status is cleaned by evaluating this column and it made dirty by setting (new), resetting (delete) or changing (updating) the formula.
	 * It is an aggregated status for new, deleted or changed formulas.
	 * It is own status of this columns only (not inherited/propagated).
	 */
/* Since we do not deal with formulas, we do not track their status of any kind - it has to be replaced by data status
	public boolean isFormulaDirty() {
		return this.formulaChange || this.formulaNew || this.formulaDelete;
	}
	public void setFormulaClean() {
		this.formulaChange = false;
		this.formulaNew = false;
		this.formulaDelete = false;
	}

	private boolean formulaChange = false; // Formula has been changed
	public void setFormulaChange(boolean dirty) {
		this.formulaChange = dirty;
	}
	private boolean formulaNew = false; // Formula has been added
	public void setFormulaNew(boolean dirty) {
		this.formulaNew = dirty;
	}
	private boolean formulaDelete = false; // Formula has been deleted
	public void setFormulaDelete(boolean dirty) {
		this.formulaDelete = dirty;
	}
*/

	//
	// Formula (translate) dependencies
	//
	
	/**
	 * All other columns it directly depends on. These columns are directly used in its formula to compute output.
	 */
	private List<Column> dependencies = new ArrayList<Column>();
	public List<Column> getDependencies() {
		return this.dependencies;
	}
	private void setDependencies(List<Column> deps) {
		resetDependencies();
		this.dependencies.addAll(deps);
	}
	private void resetDependencies() {
		this.dependencies.clear();
	}

	private List<Column> getDependencies(List<Column> cols) { // Get all unique dependencies of the specified columns (expand dependence tree nodes)
		List<Column> ret = new ArrayList<Column>();
		for(Column col : cols) {
			List<Column> deps = col.getDependencies();
			for(Column d : deps) {
				if(!ret.contains(d)) ret.add(d);
			}
		}
		return ret;
	}

	protected boolean isStartingColumn() { // True if this column has no dependencies (e.g., constant expression) or is free (user, non-derived) column
		if(!this.isDerived()) {
			return true;
		}
		else if(this.dependencies.isEmpty()) {
			return true;
		}
		else {
			return false;
		}
	}

	public BistroError getDependenceError() { // =canEvaluate. Return one error in the dependencies (recursively) including cyclic dependency error
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return new BistroError(BistroErrorCode.DEPENDENCY_CYCLE_ERROR, "Cyclic dependency.", "This column formula depends on itself directly or indirectly.");
				}
				BistroError de = dep.getError();
				if(de != null && de.code != BistroErrorCode.NONE) {
					return new BistroError(BistroErrorCode.TRANSLATE_PROPAGATION_ERROR, "Error in column " + dep.getName(), "This column formula depends on a column with errors.");
				}
			}
		}
		return null;
	}
	public BistroError getThisOrDependenceError() {
		BistroError ret = this.getError();
		if(ret != null && ret.code != BistroErrorCode.NONE) {
			return ret; // Error in this column
		}
		return this.getDependenceError();
	}

	public boolean isDependenceDirty() { // =needEvaluate. Inherited dirty status
		for(List<Column> deps = this.getDependencies(); deps.size() > 0; deps = this.getDependencies(deps)) { // Loop on expansion layers of dependencies
			for(Column dep : deps) {
				if(dep == this) {
					return true; // Cyclic dependency is also an error and hence dirty
				}
				BistroError de = dep.getError();
				if(de != null && de.code != BistroErrorCode.NONE) {					return true; // Any error must be treated as dirty status (propagated further down)
				}
				if(dep.isChanged) return true;
			}
		}
		return false; // All dependencies are up-to-date
	}
	public boolean isThisOrDependenceDirty() {
		return this.isChanged || this.isDependenceDirty();
	}


    //
    // Errors
    //

    private List<BistroError> errors = new ArrayList<BistroError>();
    public List<BistroError> getErrors() { // Empty list in the case of no errors
        return this.errors;
    }
    public BistroError getError() {
        if(getErrors().size() == 0) return null;
        return this.errors.get(this.errors.size()-1);
    }
    public boolean hasErrors() {
        if(getErrors().size() == 0) return false;
        else return true;
    }

    // TODO: Simply UDE interface by leaving one error type etc.
    // TODO: Simply UDE interface by removing translate and evaluate. Only evaluate. Translation is done etiher in construtor or when formula is set. Think about setting formula only in a setter (not in constructor).
    // TODO: UDE currently has two versions of parameter paths: names and objects. Do we really need both of them?
    // TODO: Split UdeJava into two Udes and one base UdeExprBase
    //   UdeEvalex, UdeExp4j, UdeMathparser, UdeJavaScript extend UdeExprBase - specific are type conversion (e.g., from strings) or dynamic typing like JS
    // TODO: Introduce constant UDE, for example, for initializers like 0.0 and EQUAL UDE for copying field/path values to output without any expression.
    // TODO: Define UdeLambda and then the corresponding calc/link/accu methods with lambda as parameters
    //   UdeLambda(lambda, paramPaths)

    // TODO: 2) Currently definitions are used becaues they have the logic of formula translation and evaluator creation.
    //   Think about removing them completely definitions. Indeed, we create them temporarily (as a wrapper), translate and then delete

    // TODO: Rework dependency management. Probably make the protected (they are needed only for evaluation). Simplilfy graph computation: to evaluate this column, we search for dirty (isChanged) and hasErrors.

    // ISSUE: Currently, link dependencies include also output table columns (lhs columns). If we link to them then they have to be computed (is it really so - they might be USER column). If we append then we do not care - we will append anyway.
    //   For example, can we link to derived (calculated) columns in the output table? Does it make sense?

    // Patterns:
    // 1) calculate(Ude.class/Ude.name, formula) <- here we provide translator plus proceudre/binding (new instance will be given formula string)
    // 2) calculate(lambda/Ude.class/Ude.name, path_objects/names) <- here we provide procedure plus binding (instance will be given parameter paths)

    //
    // Calcuate. Convert input parameters into an Evaluator object and ealuate it in the case of immediate (eager) action.
    //

    public void calculate(UDE ude) { // Provide instance of custom UDE which has already paths
        this.evaluator = null;
        this.resetDependencies();

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.evaluator != null && !this.hasErrors()) {
            this.kind = ColumnKind.CALC;
            this.setDependencies(this.evaluator.getDependencies());
        }
    }

    public void calculate(String clazz, String formula) { // Specify UDE class/selector and formula parameter for this class
        this.evaluator = null;
        this.resetDependencies();

        if(clazz.equals("EXP4J")) {

            // TODO: It is workaround - remove definition by copying its translation mechanism
            ColumnDefinitionCalc def = new ColumnDefinitionCalc(formula, ExpressionKind.EXP4J);

            this.evaluator = def.translate(this);

            this.errors.addAll(def.getErrors());
            if(this.evaluator != null && !this.hasErrors()) {
                this.kind = ColumnKind.CALC;
                this.setDependencies(this.evaluator.getDependencies());
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
    }

    public void calculate(Class clazz, List<ColumnPath> paths) { // Specify UDE class and parameter paths
        this.evaluator = null;
        this.resetDependencies();

        // Instantiate
        UDE ude = null;
        try {
            Constructor[] ccc = clazz.getConstructors();
            ude = (UDE)clazz.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        ude.setResolvedParamPaths(paths); // Set parameter paths (or use constructor)

        this.evaluator = new ColumnEvaluatorCalc(this, ude); // Create evaluator

        if(this.evaluator != null && !this.hasErrors()) {
            this.kind = ColumnKind.CALC;
            this.setDependencies(this.evaluator.getDependencies());
        }
    }

    //
    // Link
    //

    public void link(List<Column> columns, List<UDE> udes) { // Custom rhs UDEs for each lhs column
        this.evaluator = null;
        this.resetDependencies();

        this.evaluator = new ColumnEvaluatorLink(this, columns, udes);

        if(this.evaluator != null && !this.hasErrors()) {
            this.kind = ColumnKind.CALC;
            this.setDependencies(this.evaluator.getDependencies());
        }
    }

    public void link(String clazz, List<String> names, List<String> formulas) { // Column names in the output table and expressions in the input table
        this.evaluator = null;
        this.resetDependencies();

        if(clazz.equals("EQUAL")) {
            ; // TODO: Implement UdeEqual expression which simply returns value of the specified column
        }
        else if(clazz.equals("EXP4J")) {

            // TODO: It is workaround - remove definition by copying its translation mechanism from ColumnDefinition::translate()
            String tupleFormula = "{";
            for(int i=0; i<names.size(); i++) {
                tupleFormula += "["+names.get(i)+"]="+formulas.get(i) + ";";
            }
            tupleFormula = tupleFormula.substring(0,tupleFormula.length()-1);
            tupleFormula += "}";
            ColumnDefinitionLink def = new ColumnDefinitionLink(tupleFormula, ExpressionKind.EXP4J);

            this.evaluator = def.translate(this);

            this.errors.addAll(def.getErrors());
            if(this.evaluator != null && !this.hasErrors()) {
                this.kind = ColumnKind.CALC;
                this.setDependencies(this.evaluator.getDependencies());
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
    }

    //
    // Accumulate
    //

    public void accumulate(UDE initUde, UDE accuUde, UDE finUde, ColumnPath accuPath) { // Provide instance of custom UDEs which have already paths
        this.evaluator = null;
        this.resetDependencies();

        this.evaluator = new ColumnEvaluatorAccu(this, initUde, accuUde, finUde, accuPath);

        if(this.evaluator != null && !this.hasErrors()) {
            this.kind = ColumnKind.CALC;
            this.setDependencies(this.evaluator.getDependencies());
        }
    }

    public void accumulate(String clazz, String initFormula, String accuFormula, String finFormula, String tableName, NamePath groupPath) { // Specify UDE class/selector and formulas
        this.evaluator = null;
        this.resetDependencies();

        if(clazz.equals("EXP4J")) {

            // TODO: It is workaround - remove definition by copying its translation mechanism
            String path = "{";
            for(int i=0; i<groupPath.names.size(); i++) {
                path += "["+groupPath.names.get(i)+"].";
            }
            path = path.substring(0,path.length()-1);
            ColumnDefinitionAccu def = new ColumnDefinitionAccu(initFormula, accuFormula, finFormula, tableName, path, ExpressionKind.EXP4J);

            this.evaluator = def.translate(this);

            this.errors.addAll(def.getErrors());
            if(this.evaluator != null && !this.hasErrors()) {
                this.kind = ColumnKind.CALC;
                this.setDependencies(this.evaluator.getDependencies());
            }
        }
        else {
            ; // TODO: Error - UDE class not available/implemented
        }
    }

    //
    // Evaluate
    //

    ColumnEvaluator evaluator; // It is built by calculate-link-accumulate methods (in the case of success)

    public void evaluate() {
        this.errors.clear(); // TODO: It must be clean before we can start evaluation - otherwise evaluation cannot start

        if(this.getKind() == ColumnKind.NONE) {
            ;
        }
        // TODO: All branches are the same - merge them into one branch
        else if(this.getKind() == ColumnKind.CALC) {
            this.evaluator.evaluate();
            this.errors.addAll(this.evaluator.getErrors());
        }
        else if(this.getKind() == ColumnKind.LINK) {
            this.evaluator.evaluate();
            this.errors.addAll(this.evaluator.getErrors());
        }
        else if(this.getKind() == ColumnKind.ACCU) {
            this.evaluator.evaluate();
            this.errors.addAll(this.evaluator.getErrors());
        }

        this.isChanged = false;
    }

/*
	public void translate() {

		this.translateErrors.clear();
		this.resetDependencies();

		List<Column> columns = new ArrayList<Column>();
		
		// Translate depending on the formula kind
		if(this.kind == ColumnKind.CALC) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorCalc = (ColumnEvaluatorCalc) this.definitionCalc.translate(this);
				this.translateErrors.addAll(definitionCalc.getErrors());
			}
			if(this.evaluatorCalc != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorCalc.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == ColumnKind.LINK) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorLink = (ColumnEvaluatorLink) this.definitionLink.translate(this);
				this.translateErrors.addAll(definitionLink.getErrors());
			}
			if(this.evaluatorLink != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorLink.getDependencies()); // Dependencies
			}
		}
		else if(this.kind == ColumnKind.ACCU) {
			if(this.expressionKind != ExpressionKind.NONE) {
				this.evaluatorAccu = (ColumnEvaluatorAccu) this.definitionAccu.translate(this);
				this.translateErrors.addAll(definitionAccu.getErrors());
			}
			if(this.evaluatorAccu != null && !this.hasTranslateErrors()) {
				columns.addAll(this.evaluatorAccu.getDependencies()); // Dependencies
			}
		}

		this.setDependencies(columns);
	}
*/

	//
	// Serialization and construction
	//

	@Override
	public String toString() {
		return "[" + getName() + "]: " + input.getName() + " -> " + output.getName();
	}
	
	@Override
	public boolean equals(Object aThat) {
		if (this == aThat) return true;
		if ( !(aThat instanceof Table) ) return false;
		
		Column that = (Column)aThat;
		
		if(!that.getId().toString().equals(id.toString())) return false;
		
		return true;
	}

	public Column(Schema schema, String name, Table input, Table output) {
		this.schema = schema;
		this.id = UUID.randomUUID();
		this.name = name;
		this.input = input;
		this.output = output;
		
		// Formula
		this.kind = ColumnKind.NONE;

		// Data
		this.data = new ColumnData(this.input.getIdRange().start, this.input.getIdRange().end);
	}
}
