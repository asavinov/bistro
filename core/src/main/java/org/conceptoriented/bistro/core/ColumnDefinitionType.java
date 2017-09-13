package org.conceptoriented.bistro.core;

public enum ColumnDefinitionType {
	NONE(0), // No computations
	CALC(60), // Calculate column
	LINK(70), // Link column
	ACCU(90), // Accumulate column
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
