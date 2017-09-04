package org.conceptoriented.bistro.core.deprecated;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;

import org.conceptoriented.bistro.core.Column;

/**
 * Sum of two numeric columns.
 */
public class SUM extends EvaluatorBase {

	Column column1;
	Column column2;
	
	@Override
	public void setColumns(List<Column> columns) {
		column1 = columns.get(0);
		column2 = columns.get(1);
	}

	@Override
	public void evaluate(long row) {
		Object value1 = column1.getValue(row);
		if(value1 instanceof String) try { value1 = NumberFormat.getInstance(Locale.US).parse((String)value1); } catch (ParseException e) { value1 = null; }
		Object value2 = column2.getValue(row);
		if(value2 instanceof String) try { value2 = NumberFormat.getInstance(Locale.US).parse((String)value2); } catch (ParseException e) { value2 = null; }
		
		if(value1 == null) value1 = Double.NaN;
		if(value2 == null) value2 = Double.NaN;

		double result = Double.NaN;
		result = ((Number)value1).doubleValue() + ((Number)value2).doubleValue();
		
		thisColumn.setValue(row, result);
	}

}
