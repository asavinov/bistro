package org.conceptoriented.bistro.core;

import java.util.Arrays;
import java.util.List;

class ColumnDefinitionProj extends ColumnDefinitionLink {

    void validate() {

        // Output table must be product-table (cannot be noop-table). It could be a warning because it does not prevent from evaluating/populating.
        if(this.column.getOutput().getDefinitionType() == TableDefinitionType.NOOP) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "Proj-column must have product-table as type. Change to either link-column or product-table."));
        }

        // Check that all specified keys are really key columns of the type table
        Column nonKeyColumn = null;
        for(Column col : this.keyColumns) {
            if(!col.isKey()) {
                nonKeyColumn = col;
                break;
            }
        }
        if(nonKeyColumn != null) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "All keys in the project-column definition must be key columns of the output product-table."));
        }
    }

    public ColumnDefinitionProj(Column column, ColumnPath[] valuePaths, Column[] keyColumns) {
        super(column, valuePaths, keyColumns);

        // Use all existing keys by default if not specified
        if(keyColumns == null || keyColumns.length == 0) {
            this.keyColumns = column.getOutput().getKeyColumns();
        }

        this.isProj = true;
        this.validate();
    }

    public ColumnDefinitionProj(Column column, Column[] valueColumns, Column[] keyColumns) {
        super(column, valueColumns, keyColumns);

        // Use all existing keys by default if not specified
        if(keyColumns == null || keyColumns.length == 0) {
            this.keyColumns = column.getOutput().getKeyColumns();
        }

        this.isProj = true;
        this.validate();
    }

    public ColumnDefinitionProj(Column column, ColumnPath valuePath) {
        super(column, valuePath);

        this.isProj = true;
        this.validate();
    }
}
