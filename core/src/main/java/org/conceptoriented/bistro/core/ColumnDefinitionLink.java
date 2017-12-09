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

        //
        // Prepare value paths/exprs for search/find
        //
        //List<List<ColumnPath>> rhsParamPaths = new ArrayList<>();
        //List<Object[]> rhsParamValues = new ArrayList<>();
        List<Object> rhsResults = new ArrayList<>(); // Record of value paths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each key expression
        for(ColumnPath path : this.valuePaths) {
            //int paramCount = expr.getParameterPaths().size();

            //rhsParamPaths.add( expr.getParameterPaths() );
            //rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add(null);
        }

        //
        // Prepare value paths for where evaluation (see also TableDefinitionProd)
        //
        Expression whereExpr = typeTable.expressionWhere;
        List<ColumnPath> wherePaths =  null;
        Object[] whereValues = null;
        int[] wherePathsKeyIndex = null;
        if(whereExpr != null) {
            wherePaths =  whereExpr.getParameterPaths();
            whereValues = new Object[wherePaths.size() + 1];

            wherePathsKeyIndex = new int[wherePaths.size()];
            for(int i=0; i < wherePathsKeyIndex.length; i++) {
                Column firstSegment = wherePaths.get(i).columns.get(0); // First segment
                int keyColumnIndex = this.keyColumns.indexOf(firstSegment); // Same index is used in the record to be added
                wherePathsKeyIndex[i] = keyColumnIndex;
                // TODO: Problem: here we use only key columns filled by the proj defintion - but whereExpr might use other (key or non-key) columns
                //   Solution: we need to error-check that all key columns (in param paths) used in whereExpr, are used as key columns in proj-column defintion (when one of these definitions is added)
                // Assumption: all key columns used in params of whereExpr are also used in/coverd by proj-column keyColumns
            }

        }

        boolean whereTrue = true; // Where result for the last appended record

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int keyNo = 0; keyNo < this.keyColumns.size(); keyNo++) {

                // Read one columnPath
                Object result = this.valuePaths.get(keyNo).getValue(i);

                rhsResults.set(keyNo, result);
            }

            // TODO: We need to generalize the logic of testing for using in prod-table (TableDefinitionProd) and proj-columns (ColumnDefinitionLink)
            //   Some method generate a record with values with the purpose to add it. We also have an array of columns for this record attributes
            //   Task: check if addition is possible by evaluating whereExpr.
            //   Solution: We need to feed the record values to whereExpr paramPaths (skip first segment), taking into account the mapping (errors possible), and retrieving the param values
            //   Challenge: we need to prepare to do it multiple times and not one time. So mapping or convention is needed to map values in the record to the whereExpr param paths.

            // So we have:
            // - exists/find predicate (which returns an index if found, and also can add if the flag is set)
            // - satisfies/canAdd (which returns boolean, and can add if flag is set)
            // Maybe find() method can also check the conditions before adding (but then the question is whether check conditions before search or after search)
            // 1. We should check where condition before finding, because it is easier - finding is difficult.
            // 2. We check condition only if want to append (isProj is true)
            // 3. We read values of columns path parameters for checking a condition, so we need to keep this list and read values for each new record
            // 4. Problem is that we want to check where before adding. In prod-table, we still add, then check, and then delete or reuse if not satisfied (with negative index)
            //   We can add, then check, then remove, then find (we need to remove because otherwise find() will always find it).
            //   Alternatively, find method could be adapted to ignore the last element, but it is difficult in future with indexes etc.
            //   So the only principled solution is to be able to check conditions on a non-added record.
            //   Expr actually takes only values (so it will work) -- the problem is how to retrieve param path values for a non-existing record. The first segment of param paths is what added values are - we need to continue them to next segments.

            if(typeTable.expressionWhere != null) {

                // Read all parameters
                for(int p=0; p < wherePaths.size(); p++) {
                    int keyNo = wherePathsKeyIndex[p];
                    Object recordValue = rhsResults.get(keyNo);
                    whereValues[p] = wherePaths.get(p).getValueSkipFirst(recordValue);
                }

                // Evaluate
                try {
                    whereTrue = (boolean) whereExpr.eval(whereValues);
                }
                catch(BistroError e) {
                    this.definitionErrors.add(e);
                    return;
                }
                catch(Exception e) {
                    this.definitionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
                    return;
                }
            }
            if(!whereTrue) continue;

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

        //
        // Prepare value paths/exprs for search/find
        //
        List<List<ColumnPath>> rhsParamPaths = new ArrayList<>();
        List<Object[]> rhsParamValues = new ArrayList<>();
        List<Object> rhsResults = new ArrayList<>(); // Record of valuePaths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each key expression
        for(Expression expr : this.valueExprs) {
            int paramCount = expr.getParameterPaths().size();

            rhsParamPaths.add( expr.getParameterPaths() );
            rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add(null);
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int keyNo = 0; keyNo < this.keyColumns.size(); keyNo++) {

                List<ColumnPath> paramPaths = rhsParamPaths.get(keyNo);
                Object[] paramValues = rhsParamValues.get(keyNo);

                // Read all parameter valuePaths (assuming that this column output is not used in link keyColumns)
                int paramNo = 0;
                for(ColumnPath paramPath : paramPaths) {
                    paramValues[paramNo] = paramPath.getValue(i);
                    paramNo++;
                }

                // Evaluate this key expression
                Expression expr = this.valueExprs.get(keyNo);
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

                rhsResults.set(keyNo, result);
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
