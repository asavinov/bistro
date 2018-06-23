package org.conceptoriented.bistro.core;

class OpProject extends OpLink {

    @Override
    public OperationType getOperationType() {
        return OperationType.PROJECT;
    }

    void validate() {

        // Output table must be product-table (cannot be noop-table). It could be a warning because it does not prevent from evaluating/populating.
        if(this.column.getOutput().getOperationType() == OperationType.NOOP) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column operation error.", "Proj-column must have product-table as type. Change to either link-column or product-table."));
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
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Column operation error.", "All keys in the project-column operation must be key columns of the output product-table."));
        }
    }

    public OpProject(Column column, ColumnPath[] valuePaths, Column[] keyColumns) {
        super(column, valuePaths, keyColumns);

        // Use all existing keys by default if not specified
        if(keyColumns == null || keyColumns.length == 0) {
            this.keyColumns = column.getOutput().getKeyColumns();
        }

        this.isProj = true;
        this.validate();
    }

    public OpProject(Column column, Column[] valueColumns, Column[] keyColumns) {
        super(column, valueColumns, keyColumns);

        // Use all existing keys by default if not specified
        if(keyColumns == null || keyColumns.length == 0) {
            this.keyColumns = column.getOutput().getKeyColumns();
        }

        this.isProj = true;
        this.validate();
    }

    public OpProject(Column column, ColumnPath valuePath) {
        super(column, valuePath);

        this.isProj = true;
        this.validate();
    }
}
