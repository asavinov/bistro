package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;

public class TableDefinitionRange implements TableDefinition {

    Table table; // Used only if materialized as a table

    // It is a point on the axis the counting starts from (not necessarily included in the range)
    // It could be some date in calendar (e.g., midnight) or it could be a mean value of a variable when producing quantiles
    // It also serves to start counting interval numbers (each interval has an integer number)
    Object origin;

    // It is how the size of each step is computed.
    // Normally it is a constant value but it also could vary, e.g., in the case of date periods (like month).
    // It also could be some rule for computing the length or it could be an explicit list of lengths
    Object period;

    // How many periods (records) to generate
    Object start; // Integer value means (minimum, inclusive) interval number
    Object end; // Integer value means (maximum, exclusive) interval number

    // Options for interpreting an exact position of the interval and the corresponding conditions
    Object closed; // Which end is closed: left (default) or right
    Object label; // Does the point represents left end (default) or right end

    protected Column getRangeColumn() {
        List<Column> columns = this.table.getColumns();

        return columns.get(0);
    }
    protected Column getIntervalColumn() {
        List<Column> columns = this.table.getColumns();

        return columns.get(1);
    }

    protected  boolean isDatetimeRange() {
        return false;
    }
    protected  boolean isNumberRange() {

        // Origin value represents the type of the raster values
        if(!(this.origin instanceof Number)) {
            return false;
        }

        if(!(this.period instanceof Number)) {
            return false;
        }

        if(!(this.start instanceof Number) || !(this.end instanceof Number)) {
            return false;
        }

        return true;
    }

    void validate() {

        if(this.isDatetimeRange()) {
            ; // ZODO:
        }
        else if(this.isNumberRange()) {
            // At least one numeric column is needed to store the range values
            Column rasterColumn = this.getRangeColumn();
            if(rasterColumn == null) {
                this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A range table must have at least one noop column for storing range values."));
            }

            // This columns must be primitive one
            if(!rasterColumn.getOutput().isPrimitive()) {
                this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A column for storing range values must be a primitive column."));
            }

            // This column has to be noop
            if(rasterColumn.getDefinitionType() != ColumnDefinitionType.NOOP) {
                this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A column for storing range values must be NOOP column."));
            }
        }
        else {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "Cannot determine range type (numeric or datetime)."));
        }
    }

    List<BistroError> errors = new ArrayList<>();

    @Override
    public List<BistroError> getErrors() {
        return this.errors;
    }

    @Override
    public List<Element> getDependencies() {
        List<Element> ret = new ArrayList<>();

        // TODO:

        return ret;
    }

    @Override
    public void populate() {
        if(this.isDatetimeRange()) {
            this.populateDatetime();
        }
        else if(this.isNumberRange()) {
            this.populateNumber();
        }
        else {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "Cannot determine range type (numeric or datetime)."));
        }
    }

    public void populateDatetime() {
        // TODO:
    }
    long addDate() { // Append a new date interval
        // TODO:
        return -1;
    }

    public void populateNumber() {

        // Find columns to be set during population
        Column rasterColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        // Prepare parameters by converting or casting
        Double originValue = ((Number)this.origin).doubleValue();
        Double intervalPeriod = ((Number)this.period).doubleValue();

        // Constraint
        long intervalCount = ((Number)this.end).longValue();

        //
        // Generate all intervals
        //

        Double intervalValue = originValue;
        long intervalNo = 0;

        // Start from 0 and continue iterating till the end is detected
        while(true) {

            // Check constraint: dif the current interval is end
            boolean isEnd = ! (intervalNo < intervalCount);
            if(isEnd) {
                break;
            }

            // Append a new interval to the table
            long id = this.table.add();
            rasterColumn.setValue(id, intervalValue);
            if(intervalColumn != null) {
                intervalColumn.setValue(id, intervalNo);
            }

            // Iterate to the next interval
            intervalValue += intervalPeriod;
            intervalNo++;
        }

    }

    long addNumber() { // Append a new number interval

        // Find columns to be set during population
        Column rasterColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        // Prepare parameters by converting or casting
        Double originValue = ((Number)this.origin).doubleValue();
        Double intervalPeriod = ((Number)this.period).doubleValue();

        // Constraint
        long intervalCount = ((Number)this.end).longValue();

        //
        // Generate one interval
        //

        Double intervalValue = originValue;
        long intervalNo = 0;
        if(this.table.getLength() > 0) {
            intervalValue = (Double) rasterColumn.getValue(this.table.getIdRange().end - 1);
            if(intervalColumn != null) {
                intervalNo = (long) intervalColumn.getValue(this.table.getIdRange().end - 1);
            }

            // Iterate to the next interval
            intervalValue += intervalPeriod;
            intervalNo++;
        }

        // Check constraint: dif the current interval is end
        boolean isEnd = ! (intervalNo < intervalCount);
        if(isEnd) {
            return -1;
        }

        // Append a new interval to the table
        long id = this.table.add();
        rasterColumn.setValue(id, intervalValue);
        if(intervalColumn != null) {
            intervalColumn.setValue(id, intervalNo);
        }

        return id;
    }

    public TableDefinitionRange(Table table, Object origin, Object period, Long length) {
        this.table = table;

        this.origin = origin;
        this.period = period;

        this.start = (Long) 0L;
        this.end = (Long) length;
    }
}
