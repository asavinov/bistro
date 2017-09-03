package org.conceptoriented.bistro.core;

import java.util.List;

/**
 * User defined expression. It knows how to computing one output value given several input values.
 * Normally it is implemented by a user class programmatically or produced by a translator from some syntactic representation (formula).
 *  
 * An instance of this class provides a method for computing output value given several input values.
 * 
 * The evaluate method is unaware of where the inputs and the output value come from (that is, it is unaware of columns). 
 * However, it is assumed that these input values are read from some column paths for the current input id by the column evaluate procedure.
 * 
 * An external procedure has to configure each instance by specifying all column paths which correspond to the input values as well as output column name corresponding to the output value.
 * These parameters are then used by two purposes. They are used by the translate procedure to manage dependencies and by the column evaluate procedure to read the inputs and write the output.
 * The first parameter is always the current output and the first column path is always an output column. 
 * 
 * Limitations:
 * - It is not possible to vary input ids just because this evaluate interface is unaware of the ids and their semantics. The evaluate method will not get any id - it will get only output values and return an output value.
 * - The evaluate method does not have access to schema elements like columns and hence cannot read/write data directly, append/search records.
 *  
 * Questions/problems:
 * - How to deal with link columns. One solution is to use one evaluator object for one element in the tuple by returning only primitive value.
 * - Is it possible to reuse evaluators in nested manner? For example, if an evaluator implements a library function then it would be interesting to use these functions in other expressions.
 * 
 * Use cases:
 * - Accumulate. Here the first parameter is explicitly used as the current output and then the updated output is returned. Separate evaluators are used for initialization and finalization.
 * - Link. Approach 1: Separate evaluators are used for each member of the tuple. However, their return values are not stored but rather are used to find/append an element by the column evaluator.
 * - Translation of source expression. An instance of this class is returned by a translator from some expression syntax. For each syntax, there is one translator. The paths can be encoded into the source expressions.
 *   The result is a native expression using its native variables.
 * 
 */
public interface UDE {

	/**
	 * For each instance, its parameters are bound to certain column paths (normally primitive).
	 * This information is used by the evaluation procedure to retrieve the values and pass them as parameters to this evaluator. This procedure gets this list, retrieve these path values given the current input, and passes these values to the evaluator.
	 * Each instance has to be bound to certain paths by an external procedure. Typically, it is done by translating a formula.
	 */
	public void setParamPaths(List<NamePath> paths);
	public List<NamePath> getParamPaths();
	public List<List<Column>> getResolvedParamPaths();
	/**
	 * Each parameter has a description which can be retrieved by means of this method. 
	 * It is not the best approach because these descriptions are language specific.
	 */
	//public List<String> getParamDescriptions();


	public void translate(String formula);
	public List<BistroError> getTranslateErrors();

	/**
	 * Compute output value using the provide input values. 
	 * The first parameter is the current output value (or null).
	 * Note that all parameters are output values of different paths for one and the same input id.
	 */
	public Object evaluate(Object[] params, Object out);
	public BistroError getEvaluateError();
}
