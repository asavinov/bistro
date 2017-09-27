package org.conceptoriented.bistro.core;

public enum ColumnDefinitionType {
	NONE(0), // No computations
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
	    return ColumnDefinitionType.NONE;
	 }

	private ColumnDefinitionType(int value) {
		this.value = value;
	}
}
