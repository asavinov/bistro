package org.conceptoriented.bistro.core;

class ColumnDefinitionProj extends ColumnDefinitionLink {

    void validate() {

        // Output table must be prod-table (cannot be noop-table). It could be a warning because it does not prevent from evaluating/populating.
        if(this.column.getOutput().getDefinitionType() == TableDefinitionType.NOOP) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "Proj-column must have prod-table as type. Change to either link-column or prod-table."));
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
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column definition error.", "All keys in the proj-column definition must be key columns of the output prod-table."));
        }
    }

    public ColumnDefinitionProj(Column column, ColumnPath[] valuePaths, Column[] keyColumns) {
        super(column, valuePaths, keyColumns);
        this.isProj = true;
        this.validate();
    }

    public ColumnDefinitionProj(Column column, Column[] valueColumns, Column[] keyColumns) {
        super(column, valueColumns, keyColumns);
        this.isProj = true;
        this.validate();
    }

    public ColumnDefinitionProj(Column column, Expression[] valueExprs, Column[] keyColumns) {
        super(column, valueExprs, keyColumns);
        this.isProj = true;
        this.validate();
    }
}
