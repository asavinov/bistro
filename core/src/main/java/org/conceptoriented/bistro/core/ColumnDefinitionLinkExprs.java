package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It is an implementation of definition for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnDefinitionLinkExprs implements ColumnDefinition {

    Column column;

    List<Column> columns = new ArrayList<>();
    List<Expression> exprs = new ArrayList<>();

    List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

	@Override
	public void eval() {
        this.evaluateLink(this.columns, this.exprs);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<>();

        for (Column col : this.columns) {
            if (!ret.contains(col)) ret.add(col);
        }

        for (Expression expr : this.exprs) {
            List<Column> deps = ColumnPath.getColumns(expr.getParameterPaths());
            for (Column col : deps) {
                if (!ret.contains(col)) ret.add(col);
            }
        }

		return ret;
	}

    protected void evaluateLink(List<Column> columns, List<Expression> exprs) {

        definitionErrors.clear(); // Clear state

        Table typeTable = this.column.getOutput();

        Table mainTable = this.column.getInput();
        // Currently we make full scan by re-evaluating all existing input ids
        Range mainRange = this.column.getInput().getIdRange();

        // Each item in this lists is for one member expression
        // We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.

        List< List<ColumnPath> > rhsParamPaths = new ArrayList<>();
        List< Object[] > rhsParamValues = new ArrayList<>();
        List< Object > rhsResults = new ArrayList<>(); // Record of paths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(Expression expr : exprs) {
            int paramCount = expr.getParameterPaths().size();

            rhsParamPaths.add( expr.getParameterPaths() );
            rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int mmbrNo = 0; mmbrNo < columns.size(); mmbrNo++) {

                List<ColumnPath> paramPaths = rhsParamPaths.get(mmbrNo);
                Object[] paramValues = rhsParamValues.get(mmbrNo);

                // Read all parameter paths (assuming that this column output is not used in link columns)
                int paramNo = 0;
                for(ColumnPath paramPath : paramPaths) {
                    paramValues[paramNo] = paramPath.getValue(i);
                    paramNo++;
                }

                // Evaluate this member expression
                Expression expr = exprs.get(mmbrNo);
                Object result;
                try {
                    result = expr.evaluate(paramValues, null);
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
            Object out = typeTable.find(columns, rhsResults, true);

            // Update output
            this.column.setValue(i, out);
        }

    }

	public ColumnDefinitionLinkExprs(Column column, Column[] columns, Expression[] exprs) {
        this.column = column;

        this.columns.addAll(Arrays.asList(columns));
		this.exprs.addAll(Arrays.asList(exprs));
	}
}
