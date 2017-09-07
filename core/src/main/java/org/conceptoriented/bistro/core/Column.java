package org.conceptoriented.bistro.core;

import org.conceptoriented.bistro.core.expr.*;

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
	// Formula dependencies
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
					return new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Cyclic dependency.", "This column formula depends on itself directly or indirectly.");
				}
				BistroError de = dep.getError();
				if(de != null && de.code != BistroErrorCode.NONE) {
					return new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Error in column " + dep.getName(), "This column formula depends on a column with errors.");
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

            UDE expr = new UdeJava(formula, this.input);

            if(expr == null) {
                this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot translate expression. " + formula));
                return;
            }
            this.errors.addAll(expr.getTranslateErrors());
            if(this.hasErrors()) return;

            this.evaluator = new ColumnEvaluatorCalc(this, expr);

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
            this.kind = ColumnKind.LINK;
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
            // Resolve all names
            List<Column> columns = new ArrayList<>();
            for(String name : names) {
                Column col = this.output.getColumn(name);
                if(col == null) {
                    // TODO: Add error
                    return;
                }
                columns.add(col);
            }

            // Translate all formulas
            List<UDE> exprs = new ArrayList<>();
            for(String formula : formulas) {
                UDE expr = new UdeJava(formula, this.input);
                if(expr == null) {
                    // TODO: Add error
                    return;
                }
                this.errors.addAll(expr.getTranslateErrors());
                if(this.hasErrors()) return;
                exprs.add(expr);
            }

            this.evaluator = new ColumnEvaluatorLink(this, columns, exprs);

            if(this.evaluator != null && !this.hasErrors()) {
                this.kind = ColumnKind.LINK;
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
            this.kind = ColumnKind.ACCU;
            this.setDependencies(this.evaluator.getDependencies());
        }
    }

    public void accumulate(String clazz, String initFormula, String accuFormula, String finFormula, String accuTableName, NamePath accuLinkPath) { // Specify UDE class/selector and formulas
        this.evaluator = null;
        this.resetDependencies();

        if(clazz.equals("EXP4J")) {

            ColumnPath accuPathColumns = null;

            // Accu table and link (group) path
            Table accuTable = schema.getTable(accuTableName);
            if(accuTable == null) { // Binding error
                this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Binding error.", "Cannot find table: " + accuTableName));
                return;
            }
            accuPathColumns = accuLinkPath.resolveColumns(accuTable);
            if(accuPathColumns == null) { // Binding error
                this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Binding error.", "Cannot find columns: " + accuLinkPath.toString()));
                return;
            }

            UDE initExpr = null;

            // Initialization (always initialize - even for empty formula)
            if(initFormula == null || initFormula.isEmpty()) { // TODO: We need UDE for constants and for equality (equal to the specified column)
                initExpr = new UdeJava(this.getDefaultValue().toString(), this.input);
            }
            else {
                initExpr = new UdeJava(initFormula, this.input);
            }
            this.errors.addAll(initExpr.getTranslateErrors());
            if(this.hasErrors()) return; // Cannot proceed

            // Accumulation
            UDE accuExpr = null;
            accuExpr = new UdeJava(accuFormula, accuTable);
            this.errors.addAll(accuExpr.getTranslateErrors());
            if(this.hasErrors()) return; // Cannot proceed

            // Finalization
            UDE finExpr = null;
            if(finFormula != null && !finFormula.isEmpty()) {
                finExpr = new UdeJava(finFormula, this.input);
                this.errors.addAll(finExpr.getTranslateErrors());
                if(this.hasErrors()) return; // Cannot proceed
            }

            // Errors
            if(initExpr == null || accuExpr  == null /* || finExpr == null */) { // TODO: finExpr can be null in the case of no formula. We need to fix this and distinguis between errors and having no formula.
                String frml = "";
                if(initExpr == null) frml = initFormula;
                else if(accuExpr == null) frml = accuFormula;
                else if(finExpr == null) frml = finFormula;
                this.errors.add(new BistroError(BistroErrorCode.TRANSLATE_ERROR, "Translate error.", "Cannot create expression. " + frml));
                return;
            }

            this.errors.addAll(initExpr.getTranslateErrors());
            this.errors.addAll(accuExpr.getTranslateErrors());
            // this.errors.addAll(finExpr.getTranslateErrors()); // TODO: Fix uncertainty with null expression in the case of no formula and in the case of errors
            if(this.hasErrors()) return; // Cannot proceed

            this.evaluator = new ColumnEvaluatorAccu(this, initExpr, accuExpr, finExpr, accuPathColumns);

            if(this.evaluator != null && !this.hasErrors()) {
                this.kind = ColumnKind.ACCU;
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
        if(this.hasErrors()) { // TODO: We need to check only translate errors - if there were evaluate errors then we can retry
            return;
        }
        this.errors.clear();

        if(this.getKind() == ColumnKind.NONE) {
            return; // It is not evaluatable column
        }

        this.evaluator.evaluate(); // Concrete evaluation procedure depends on the Evaluator subclass: calc, link, accu.

        this.errors.addAll(this.evaluator.getErrors());

        this.isChanged = false; // Clean the state (remove dirty flag)
    }

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
