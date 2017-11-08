package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * This class combines one evaluator with the specification where its parameters have to be read from as an array of column valuePaths.
 * The column valuePaths allow for applying the same evaluator to different keyColumns.
 */
public interface Expression extends Evaluator {

	public void setParameterPaths(List<ColumnPath> paths);
	public List<ColumnPath> getParameterPaths();
}
