package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColumnDefinitionLink implements ColumnDefinition {

    Column column;

    List<Column> keyColumns = new ArrayList<>();

    public boolean isProj = false; // Either link-column or proj-columns. Used in sub-classes and methods as a switch

    // Two types of definition. Used in methods as a switch
    List<ColumnPath> valuePaths;
    List<Expression> valueExprs;

    List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        if(this.valuePaths != null) {
            // Dependencies are in paths
            for (ColumnPath path : this.valuePaths) {
                for (Column col : path.columns) {
                    if (!ret.contains(col)) ret.add(col);
                }
            }
        }
        else if(this.valueExprs != null) {
            // Dependencies are in expressions
            for (Expression expr : this.valueExprs) {
                List<Column> deps = ColumnPath.getColumns(expr.getParameterPaths());
                for (Column col : deps) {
                    if (!ret.contains(col)) ret.add(col);
                }
            }
        }
        else {
            return null;
        }

        if(!this.isProj) {
            // Link columns depend on the output table (in contrast to proj columns which do not)
            ret.add(this.column.getOutput());
        }

        return ret;
    }

    @Override
    public void eval() {
        if(this.valuePaths != null) {
            this.evalPaths();
        }
        else if(this.valueExprs != null) {
            this.evalExprs();
        }
    }
    protected void evalPaths() {

        definitionErrors.clear(); // Clear state

        Table typeTable = this.column.getOutput();

        Table mainTable = this.column.getInput();
        // Currently we make full scan by re-evaluating all existing input ids
        Range mainRange = this.column.getInput().getIdRange();

        // Each item in this lists is for one member expression
        // We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.

        //List< List<ColumnPath> > rhsParamPaths = new ArrayList<>();
        //List< Object[] > rhsParamValues = new ArrayList<>();
        List< Object > rhsResults = new ArrayList<>(); // Record of value paths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(ColumnPath path : this.valuePaths) {
            //int paramCount = expr.getParameterPaths().size();

            //rhsParamPaths.add( expr.getParameterPaths() );
            //rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int mmbrNo = 0; mmbrNo < this.keyColumns.size(); mmbrNo++) {

                // Read one columnPath
                Object result = this.valuePaths.get(mmbrNo).getValue(i);

                rhsResults.set(mmbrNo, result);
            }

            // Find element in the type table which corresponds to these expression results (can be null if not found and not added)
            Object out = typeTable.find(rhsResults, this.keyColumns, this.isProj);

            // Update output
            this.column.setValue(i, out);
        }

    }
    protected void evalExprs() {

        definitionErrors.clear(); // Clear state

        Table typeTable = this.column.getOutput();

        Table mainTable = this.column.getInput();
        // Currently we make full scan by re-evaluating all existing input ids
        Range mainRange = this.column.getInput().getIdRange();

        // Each item in this lists is for one member expression
        // We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.

        List< List<ColumnPath> > rhsParamPaths = new ArrayList<>();
        List< Object[] > rhsParamValues = new ArrayList<>();
        List< Object > rhsResults = new ArrayList<>(); // Record of valuePaths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(Expression expr : this.valueExprs) {
            int paramCount = expr.getParameterPaths().size();

            rhsParamPaths.add( expr.getParameterPaths() );
            rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int mmbrNo = 0; mmbrNo < this.keyColumns.size(); mmbrNo++) {

                List<ColumnPath> paramPaths = rhsParamPaths.get(mmbrNo);
                Object[] paramValues = rhsParamValues.get(mmbrNo);

                // Read all parameter valuePaths (assuming that this column output is not used in link keyColumns)
                int paramNo = 0;
                for(ColumnPath paramPath : paramPaths) {
                    paramValues[paramNo] = paramPath.getValue(i);
                    paramNo++;
                }

                // Evaluate this member expression
                Expression expr = this.valueExprs.get(mmbrNo);
                Object result;
                try {
                    result = expr.eval(paramValues);
                } catch (BistroError e) {
                    definitionErrors.add(e);
                    return;
                }
                catch(Exception e) {
                    this.definitionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                    return;
                }

                rhsResults.set(mmbrNo, result);
            }

            // Find element in the type table which corresponds to these expression results (can be null if not found and not added)
            Object out = typeTable.find(rhsResults, this.keyColumns, this.isProj);

            // Update output
            this.column.setValue(i, out);
        }
    }

    public ColumnDefinitionLink(Column column, ColumnPath[] valuePaths, Column[] keyColumns) {
        this.column = column;

        this.keyColumns = Arrays.asList(keyColumns);

        this.valuePaths = Arrays.asList(valuePaths);
    }

    public ColumnDefinitionLink(Column column, Column[] valueColumns, Column[] keyColumns) {
        this.column = column;

        List<ColumnPath> paths = new ArrayList<>();
        for(Column col : valueColumns) {
            paths.add(new ColumnPath(col));
        }

        this.keyColumns = Arrays.asList(keyColumns);

        this.valuePaths = paths;
    }

    public ColumnDefinitionLink(Column column, Expression[] valueExprs, Column[] keyColumns) {
        this.column = column;

        this.keyColumns = Arrays.asList(keyColumns);

        this.valueExprs = Arrays.asList(valueExprs);
    }
}
