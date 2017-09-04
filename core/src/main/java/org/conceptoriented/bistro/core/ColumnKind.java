package org.conceptoriented.bistro.core;

public enum ColumnKind {
	NONE(10), // Not specified (missing)
	UNKNOWN(20), // Cannot determine

	AUTO(0), // Auto. Formula kind has to be determined automatically using other parameters. 

	// No formula (free columns). Values are provided by the user or in any case from outside, that is, not derived.
	USER(50), 

	// Calculated (row-based, no accumulation, non-complex). Return value is a member of the output (primitive) set.
	CALC(60),

	// Tuple (complex). 
	// A number of expressions assigned to one column of the output (complex) set and returning a member of this output column set.
	// This combination of values is then used look up and existing element (and optionally add)
	// One version is that the evaluator knows how to loop up (and add). Another version is that it is done externally and evaluator only evaluates a number of expressions.
	LINK(70),

	// Accumulation
	// The column is defined via initialization and finalization expression which are normal calc-formulas.
	// In addition, there is an accumulation expression.
	// One version is a normal expression which uses a special variable out by returning already updated value (so it knows how to update)
	ACCU(90),

	CLASS(100), // Java class
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static ColumnKind fromInt(int value) {
	    for (ColumnKind kind : ColumnKind.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return ColumnKind.AUTO;
	 }

	private ColumnKind(int value) {
		this.value = value;
	}
}
