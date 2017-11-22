package org.conceptoriented.bistro.core;

public enum ColumnDefinitionType {
	NOOP(0), // No definition
	CALC(10), // Calculate column
	LINK(20), // Link column
	ACCU(30), // Accumulate column
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
