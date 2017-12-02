package org.conceptoriented.bistro.core;

public enum ColumnDefinitionType {

	// No definition column
	// These columns are not overwritten automatically during evaluation. They are set and reset only via API and then retain their output during inference.
	NOOP(0),

    // Key column
    // Its outputs will be set by the table population procedure during inference for each new instance
    KEY(10),

    // Calculate column
    CALC(20),

    // Link column
    // Find an element in the type table and store it as output
    LINK(30),

    // Project column
    // Append an element to the output table (if does not exist) and store it as an output
    PROJ(40),

    // Accumulate column
    // Update the output value for each element of a different (fact) table which links to this element
    ACCU(50),
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static ColumnDefinitionType fromInt(int value) {
	    for (ColumnDefinitionType kind : ColumnDefinitionType.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return ColumnDefinitionType.NOOP;
	 }

	private ColumnDefinitionType(int value) {
		this.value = value;
	}
}
