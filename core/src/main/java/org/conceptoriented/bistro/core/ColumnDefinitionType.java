package org.conceptoriented.bistro.core;

public enum ColumnDefinitionType {

	// No definition column
	// These columns are not overwritten automatically during evaluation. They are set and reset only via API and then retain their output during inference.
	NOOP(0),

    // Calculate column
    CALC(10),

    // Link column
    // Find an element in the type table and store it as output
    LINK(20),

    // Project column
    // Append an element to the output table (if does not exist) and store it as an output
    PROJ(30),

    // Accumulate column
    // Update the output value for each element of the group
    ACCU(40),

	// Rolling column
	// Update the output value for each element of  the window taking into account the distance
	ROLL(50),
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
