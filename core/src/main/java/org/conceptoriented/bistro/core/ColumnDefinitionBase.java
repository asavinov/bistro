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
		Object[] paramValues = new Object[paramPaths.size()]; // Will store paths for all params
		Object out; // Current output value
		Object result; // Will be written to output for each input

		for(long i=mainRange.start; i<mainRange.end; i++) {
			// Find group [ACCU-specific]
			Long g = accuLinkPath == null ? i : (Long) accuLinkPath.getValue(i);

			// Read all parameter paths
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
