package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

public abstract class ColumnDefinitionBase implements ColumnDefinition { // Convenience class for implementing common functions

	Column column;

	List<BistroError> definitionErrors = new ArrayList<>();
	@Override
	public List<BistroError> getErrors() {
		return this.definitionErrors;
	}

	protected void evaluateExpr(Expression expr, ColumnPath accuLinkPath) {

		definitionErrors.clear(); // Clear state

		Table mainTable = accuLinkPath == null ? this.column.getInput() : accuLinkPath.getInput(); // Loop/scan table

		// ACCU: Currently we do full re-eval by resetting the accu column outputs and then making full scan through all existing facts
		// ACCU: The optimal approach is to apply negative accu function for removed elements and then positive accu function for added elements
		Range mainRange = mainTable.getIdRange();

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading values
		List<ColumnPath> paramPaths = expr.getParameterPaths();
		Object[] paramValues = new Object[paramPaths.size()]; // Will store values for all params
		Object out; // Current output value
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Long g = accuLinkPath == null ? i : (Long) accuLinkPath.getValue(i);

			// Read all parameter values
			int paramNo = 0;
			for(ColumnPath paramPath : paramPaths) {
				paramValues[paramNo] = paramPath.getValue(i);
				paramNo++;
			}

			// Read current out value
			out = this.column.getValue(g); // [ACCU-specific] [FIN-specific]

			// Evaluate
			try {
				result = expr.evaluate(paramValues, out);
			}
			catch(BistroError e) {
				definitionErrors.add(e);
				return;
			}

			// Update output
			this.column.setValue(g, result);
		}
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
        List< Object > rhsResults = new ArrayList<>(); // Record of values used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(Expression expr : exprs) {
            int paramCount = expr.getParameterPaths().size();

            rhsParamPaths.add( expr.getParameterPaths() );
            rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int udeNo = 0; udeNo < columns.size(); udeNo++) {

                List<ColumnPath> paramPaths = rhsParamPaths.get(udeNo);
                Object[] paramValues = rhsParamValues.get(udeNo);

                // Read all parameter values (assuming that this column output is not used in link columns)
                int paramNo = 0;
                for(ColumnPath paramPath : paramPaths) {
                    paramValues[paramNo] = paramPath.getValue(i);
                    paramNo++;
                }

                // Evaluate this member expression
                Expression expr = exprs.get(udeNo);
                Object result;
                try {
                    result = expr.evaluate(paramValues, null);
                } catch (BistroError e) {
                    definitionErrors.add(e);
                    return;
                }

                rhsResults.set(udeNo, result);
            }

            // Find element in the type table which corresponds to these expression results (can be null if not found and not added)
            Object out = typeTable.find(columns, rhsResults, true);

            // Update output
            this.column.setValue(i, out);
        }

    }

	public ColumnDefinitionBase(Column column) {
		this.column = column;
	}

}
