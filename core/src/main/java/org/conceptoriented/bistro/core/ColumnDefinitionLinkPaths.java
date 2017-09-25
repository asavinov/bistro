package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * It is an implementation of definition for link columns.
 * It loops through the main table, reads inputs, passes them to the expression and then write the output to the main column.
 */
public class ColumnDefinitionLinkPaths implements ColumnDefinition {

    Column column;

    List<Column> columns = new ArrayList<>();
    List<ColumnPath> paths = new ArrayList<>();

    List<BistroError> definitionErrors = new ArrayList<>();
    @Override
    public List<BistroError> getErrors() {
        return this.definitionErrors;
    }

	@Override
	public void eval() {
        this.evaluateLink(this.columns, this.paths);
	}

	@Override
	public List<Column> getDependencies() {
		List<Column> ret = new ArrayList<>();

        for (Column col : this.columns) {
            if (!ret.contains(col)) ret.add(col);
        }

        for (ColumnPath path : this.paths) {
            for (Column col : path.columns) {
                if (!ret.contains(col)) ret.add(col);
            }
        }

		return ret;
	}

    protected void evaluateLink(List<Column> columns, List<ColumnPath> columnPaths) {

        definitionErrors.clear(); // Clear state

        Table typeTable = this.column.getOutput();

        Table mainTable = this.column.getInput();
        // Currently we make full scan by re-evaluating all existing input ids
        Range mainRange = this.column.getInput().getIdRange();

        // Each item in this lists is for one member expression
        // We use lists and not map because want to use common index (faster) for access and not key (slower) which is important for frequent accesses in a long loop.

        //List< List<ColumnPath> > rhsParamPaths = new ArrayList<>();
        //List< Object[] > rhsParamValues = new ArrayList<>();
        List< Object > rhsResults = new ArrayList<>(); // Record of paths used for search (produced by expressions and having same length as column list)

        // Initialize these lists for each member expression
        for(ColumnPath path : columnPaths) {
            //int paramCount = expr.getParameterPaths().size();

            //rhsParamPaths.add( expr.getParameterPaths() );
            //rhsParamValues.add( new Object[ paramCount ] );
            rhsResults.add( null );
        }

        for(long i=mainRange.start; i<mainRange.end; i++) {

            // Evaluate ALL child rhs expressions by producing an array/record of their results
            for(int mmbrNo = 0; mmbrNo < columns.size(); mmbrNo++) {

                // Read one columnPath
                Object result = columnPaths.get(mmbrNo).getValue(i);

                rhsResults.set(mmbrNo, result);
            }

            // Find element in the type table which corresponds to these expression results (can be null if not found and not added)
            Object out = typeTable.find(columns, rhsResults, true);

            // Update output
            this.column.setValue(i, out);
        }

    }

    public ColumnDefinitionLinkPaths(Column column, Column[] columns, ColumnPath[] paths) {
        this.column = column;

		this.columns.addAll(Arrays.asList(columns));
		this.paths.addAll(Arrays.asList(paths));
	}

    public ColumnDefinitionLinkPaths(Column column, Column[] columns, Column[] columnPaths) {
        this.column = column;

        List<ColumnPath> paths = new ArrayList<>();
        for(Column col : columns) {
            paths.add(new ColumnPath(col));
        }

        this.columns.addAll(Arrays.asList(columns));
        this.paths.addAll(paths);
    }
}
