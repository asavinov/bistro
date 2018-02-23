package org.conceptoriented.bistro.core;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Math.floor;

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

        // All incoming (populating) proj-columns (if any)
        List<Column> projCols = this.table.getProjColumns();
        // And their input tables which have to be populated before
        List<Table> projTabs = projCols.stream().map(x -> x.getInput()).collect(Collectors.toList());

        ret.addAll(projCols);
        ret.addAll(projTabs);

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

    long addNumber(Number value) { // Append a new interval the specified value belongs to as well as all intervals between the last one

        // Find columns to be set during population
        Column rasterColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        // Prepare parameters by converting or casting
        Double originValue = ((Number)this.origin).doubleValue();
        Double intervalPeriod = ((Number)this.period).doubleValue();

        // Constraint
        long intervalCount = ((Number)this.end).longValue();

        Double intervalValue = originValue;
        long intervalNo = 0;
        long id = -1;

        //
        // Special case: empty table (no interval to append after)
        //
        if(this.table.getLength() == 0) {

            intervalNo = (long) Math.floor( ((Double)value - originValue) / intervalPeriod );
            intervalValue = originValue + intervalNo * intervalPeriod;

            // Add interval if it satisfies constraints
            if(intervalNo >= 0 && intervalNo < intervalCount) {
                // Append a new interval to the table
                id = this.table.add();
                rasterColumn.setValue(id, intervalValue);
                if(intervalColumn != null) {
                    intervalColumn.setValue(id, intervalNo);
                }
            }
        }
        //
        // Append new interval(s) after an existing interval
        //
        else {

            // Find initial (previous) interval
            id = this.table.getIdRange().end - 1;
            intervalValue = (Double) rasterColumn.getValue(id);
            if(intervalColumn != null) {
                intervalNo = (long) intervalColumn.getValue(id);
            }

            while(true) {
                // If the current interval covers the value then break
                if((double)value < intervalValue + intervalPeriod) {
                    break;
                }

                // Iterate to the next interval
                intervalValue += intervalPeriod;
                intervalNo++;

                // Add interval if it satisfies constraints
                if(intervalNo >= 0 && intervalNo < intervalCount) {
                    // Append a new interval to the table
                    id = this.table.add();
                    rasterColumn.setValue(id, intervalValue);
                    if(intervalColumn != null) {
                        intervalColumn.setValue(id, intervalNo);
                    }
                }
                else {
                    id = -1; // Does not satisfies constraints
                    break;
                }
            }
        }

        return id; // Return last appended interval - not necessarily that covering the value
    }

    public TableDefinitionRange(Table table, Object origin, Object period, Long length) {
        this.table = table;

        this.origin = origin;
        this.period = period;

        this.start = (Long) 0L;
        this.end = (Long) length;
    }
}
