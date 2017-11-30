package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * This class knows how to produce outputs of this column for all its inputs.
 * This class implements the following aspects:
 * - Iterating: the main (loop) table and other tables needed for evaluation of this column outputs
 * - Reading inputs: column valuePaths which are used to compute the output including expression parameters or group path for accumulation
 * - Writing output: how to find the output and write it to this column data
 */
public interface ColumnDefinition {
	public void eval();
	public List<BistroError> getErrors();
	public List<Element> getDependencies();
}
