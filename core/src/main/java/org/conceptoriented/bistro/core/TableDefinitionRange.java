package org.conceptoriented.bistro.core;

import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TableDefinitionRange implements TableDefinition {

    Table table; // Used only if materialized as a table

    // Determines which data type is used for the range (Double, Duration, Period etc.)
    String rangeType;

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

    protected Object getNext(Object value) {
        Object nextValue = null;

        if(this.rangeType.equals("Number")) {
            nextValue = (Double)value + (Double)this.period;
        }
        else if(this.rangeType.equals("Duration")) {
            nextValue = ((Instant)value).plus((Duration)this.period);
        }
        else if(this.rangeType.equals("Period")) {
            nextValue = ((LocalDate)value).plus((Period)this.period);
        }

        return nextValue;
    }

    // Get interval value and number the specified value belongs to
    protected Object[] getInterval(Object value) {

        Object intervalValue = null;
        Long intervalNo = -1L;

        if(this.rangeType.equals("Number")) {
            intervalNo = (long) Math.floor( ((double)value - (double)this.origin) / (double)this.period);
            intervalValue = (double)this.origin + intervalNo * (double)this.period;
        }
        else if(this.rangeType.equals("Duration")) {
            if(((Instant)value).isBefore((Instant)this.origin)) {
                ; // Not found
            }
            else {
                Instant pos = (Instant)this.origin;
                Instant next;
                for(intervalNo = 0L; intervalNo < (Long)this.end; intervalNo++) {
                    next = pos.plus((Duration)this.period);
                    if(next.isAfter((Instant)value)) {
                        break;
                    }
                    pos = next;
                }
                intervalValue = pos;
            }
        }
        else if(this.rangeType.equals("Period")) {
            if(((LocalTime)value).isBefore((LocalTime)this.origin)) {
                ; // Not found
            }
            LocalDate pos = (LocalDate)this.origin;
            LocalDate next;
            for(intervalNo = 0L; intervalNo < (Long)this.end; intervalNo++) {
                next = pos.plus((Period)this.period);
                if(next.isAfter((LocalDate)value)) {
                    break;
                }
                pos = next;
            }
            intervalValue = pos;
        }

        Object[] ret = new Object[] { intervalValue, intervalNo };

        return ret;
    }


    protected boolean inInterval(Object intervalValue, Object value) { // Check if the value is within the specified interval

        if(this.rangeType.equals("Number")) {
            double nextValue = (Double)intervalValue + (Double)this.period;

            if((double)value >= (double)intervalValue && (double)value < nextValue) {
                return true;
            }
        }
        else if(this.rangeType.equals("Duration")) {
            Instant nextValue = ((Instant)intervalValue).plus((Duration)this.period);

            if(((Instant)value).equals((Instant)intervalValue)) {
                return true;
            }
            if(((Instant)value).isAfter((Instant)intervalValue) && ((Instant)value).isBefore(nextValue)) {
                return true;
            }
        }
        else if(this.rangeType.equals("Period")) {
            LocalDate nextValue = ((LocalDate)intervalValue).plus((Period)this.period);

            if(((LocalDate)value).equals((LocalDate)intervalValue)) {
                return true;
            }
            if(((LocalDate)value).isAfter((LocalDate)intervalValue) && ((LocalDate)value).isBefore(nextValue)) {
                return true;
            }
        }

        return false;
    }

    protected Column getRangeColumn() {
        List<Column> columns = this.table.getColumns();

        return columns.get(0);
    }
    protected Column getIntervalColumn() {
        List<Column> columns = this.table.getColumns();

        return columns.get(1);
    }

    void validate() {

        // At least one numeric column is needed to store the range values
        Column rasterColumn = this.getRangeColumn();
        if(rasterColumn == null) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A range table must have at least one noop column for storing range values."));
            return;
        }

        // This columns must be primitive one
        if(!rasterColumn.getOutput().isPrimitive()) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A column for storing range values must be a primitive column."));
            return;
        }

        // This column has to be noop
        if(rasterColumn.getDefinitionType() != ColumnDefinitionType.NOOP) {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "A column for storing range values must be NOOP column."));
            return;
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

        // Find columns to be set during population
        Column rasterColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        // Start from 0 and continue iterating till the end is detected
        Object intervalValue = this.origin;
        long intervalNo = 0;
        while(true) {

            // Check constraint: dif the current interval is end
            if(intervalNo >= (long)this.end) {
                break;
            }

            // Append a new interval to the table
            long id = this.table.add();
            rasterColumn.setValue(id, intervalValue);
            if(intervalColumn != null) {
                intervalColumn.setValue(id, intervalNo);
            }

            intervalValue = this.getNext(intervalValue); // Iterate the value
            intervalNo++; // Iterate interval number
        }
    }

    // Append a new interval the specified value belongs to as well as all intervals between the last one
    protected long add(Object value) {

        // Find columns to be set during population
        Column rasterColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        // Constraint
        long intervalCount = ((Number)this.end).longValue();

        Object intervalValue = this.origin;
        long intervalNo = 0;
        long id = -1;
        if(this.table.getLength() == 0) { // Special case: empty table (no interval to append after)

            Object[] interval = this.getInterval(value);
            intervalValue = interval[0];
            intervalNo = (Long)interval[1];

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
        else { // Append new interval(s) after an existing interval

            // Find initial (previous) interval
            id = this.table.getIdRange().end - 1;
            intervalValue = rasterColumn.getValue(id);
            if(intervalColumn != null) {
                intervalNo = (long) intervalColumn.getValue(id);
            }

            while(true) {
                // If the current interval covers the value then break
                if(this.inInterval(intervalValue, value)) {
                    break;
                }

                // Iterate to the next interval
                intervalValue = this.getNext(intervalValue);
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

    // Use inequality for finding interval this object belongs to and return id of the record representing this interval
    protected long findRange(Object value, boolean append) {

        // Range tables do not have nulls or NaNs
        if(value == null) return -1;
        if(!(value instanceof Number || value instanceof Instant || value instanceof LocalDate)) {
            return -1; // Wrong use
        }

        Column rangeColumn = this.getRangeColumn();
        Column intervalColumn = this.getIntervalColumn();

        Range idRange = this.table.getIdRange();

        long index = rangeColumn.findSorted(value); // Data in a range table is supposed to be sorted

        if(index >= 0) { // If positive, then it is id of the found value
            ;
        }
        if(index < 0) { // If negatvie, then not found, and (-index-1) is id of the nearest greater value
            index = -index - 1; // Insertion index. Id of the next greater value

            if(idRange.getLength() == 0) { // Special case: no elements
                // Proj: insert interval corresponding to the value as the very first interval in the range
                index = -1;
            }
            else if(index == idRange.start) { // Before first element. Insertion in range not possible (range is supposed to be monotonically growing)
                // Proj: no insertion possible before existing intervals
                index = -1;
            }
            else if(index < idRange.end) { // Between two raster points of an existing interval
                // Proj: no insertion needed - link to the existing interval
                index = index - 1; // Closest left border
            }
            else if(index >= idRange.end) { // After last element

                Object lastValue = rangeColumn.getValue(idRange.end-1);

                boolean inInterval = this.inInterval(lastValue, value);
                if(inInterval) {
                    // Proj: no insertion needed - link to the existing interval
                    index = index - 1; // Closest left border
                }
                else { // Too high value
                    // Proj: insert interval corresponding to the value as well as all intervals before the last existing interval
                    index = -1;
                }
            }

            // If not found, and can be appended, and requested, then append the value (interval and all previous intevals before the last existing one)
            if(index < 0 && append) {
                index = this.add(value); // Add one interval as well as intervals between the last one
            }
        }

        return index;
    }

    public TableDefinitionRange(Table table, Object origin, Object period, Long length) {
        this.table = table;

        this.validate();

        // Determine type of range
        if(period instanceof Number && origin instanceof Number) {
            this.rangeType = "Number";
            this.origin = ((Number)origin).doubleValue();
            this.period = ((Number)period).doubleValue();
        }
        else if(period instanceof Duration && origin instanceof Instant) {
            this.rangeType = "Duration";
            this.origin = ((Instant)origin);
            this.period = (Duration)period;
        }
        else if(period instanceof Period && origin instanceof LocalDate) {
            this.rangeType = "Period";
            this.origin = ((LocalDate)origin);
            this.period = (Period)period;
        }
        else {
            this.errors.add(new BistroError(BistroErrorCode.DEFINITION_ERROR, "Table definition error.", "Cannot determine range data type. Use appropriate data types in parameters."));
        }

        this.start = (Long) 0L;
        this.end = (Long) length;

    }
}
