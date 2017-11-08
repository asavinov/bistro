package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It is an implementation of definition for link keyColumns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnDefinitionLinkPaths implements ColumnDefinition {

    Column column;

    List<Column> keyColumns = new ArrayList<>();

    List<ColumnPath> valuePaths = new ArrayList<>();

    List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

	@Override
	public void eval() {
        this.evaluateLink(this.keyColumns, this.valuePaths);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<>();

        for (ColumnPath path : this.valuePaths) {
            for (Column col : path.columns) {
                if (!ret.contains(col)) ret.add(col);
            }
        }

		return ret;
	}

    protected void evaluateLink(List<Column> keyColumns, List<ColumnPath> valuePaths) {

        definitionErrors.clear(); // Clear state

        Table typeTable = this.column.getOutput();

        Table mainTable = this.column.getInput();
        // Currently we make full scan by re-evaluating all existing input ids
        Range mainRange = this.column.getInput().getIdRange();

        // Each item in this lists is for one member expression
        // We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.

        //List< List<ColumnPath> > rhsParamPaths = new ArrayList<>();
        //List< Object[] > rhsParamValues = new ArrayList<>();
        List< Object > rhsResults = new ArrayList<>(); // Record of valuePaths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(ColumnPath path : valuePaths) {
            //int paramCount = expr.getParameterPaths().size();

            //rhsParamPaths.add( expr.getParameterPaths() );
            //rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int mmbrNo = 0; mmbrNo < keyColumns.size(); mmbrNo++) {

                // Read one columnPath
                Object result = valuePaths.get(mmbrNo).getValue(i);

                rhsResults.set(mmbrNo, result);
            }

            // Find element in the type table which corresponds to these expression results (can be null if not found and not added)
            Object out = typeTable.find(keyColumns, rhsResults, true);

            // Update output
            this.column.setValue(i, out);
        }

    }

    public ColumnDefinitionLinkPaths(Column column, Column[] keyColumns, ColumnPath[] valuePaths) {
        this.column = column;

		this.keyColumns.addAll(Arrays.asList(keyColumns));
		this.valuePaths.addAll(Arrays.asList(valuePaths));
	}

    public ColumnDefinitionLinkPaths(Column column, Column[] keyColumns, Column[] valueColumns) {
        this.column = column;

        List<ColumnPath> paths = new ArrayList<>();
        for(Column col : valueColumns) {
            paths.add(new ColumnPath(col));
        }

        this.keyColumns.addAll(Arrays.asList(keyColumns));
        this.valuePaths.addAll(paths);
    }
}
