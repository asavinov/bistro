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

		// Get all necessary parameters and prepare (resolve) the corresponding data (function) objects for reading paths
		List<ColumnPath> paramPaths = expr.getParameterPaths();
		Object[] paramValues = new Object[paramPaths.size() + 1]; // Will store paths for all params and current output at the end
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Object g_out = accuLinkPath == null ? i : accuLinkPath.getValue(i);
			if(g_out == null) {
			    continue; // Do not accumulate facts without group
            }
            Long g = (Long)g_out;

			// Read all parameter paths
			for(int p=0; p<paramPaths.size(); p++) {
				paramValues[p] = paramPaths.get(p).getValue(i);
			}

			// Read current out value and store as the last element of parameter array
			paramValues[paramValues.length-1] = this.column.getValue(g); // [ACCU-specific] [FIN-specific]

			// Evaluate
			try {
				result = expr.eval(paramValues);
			}
			catch(BistroError e) {
				this.definitionErrors.add(e);
				return;
			}
			catch(Exception e) {
                this.definitionErrors.add( new BistroError(BistroErrorCode.EVALUATION_ERROR, e.getMessage(), "") );
				return;
			}

			// Update output
			this.column.setValue(g, result);
		}
	}

    public ColumnDefinitionBase(Column column) {
		this.column = column;
	}

}
