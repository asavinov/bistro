package org.conceptoriented.bistro.core;

public enum DefinitionType {
	NONE(0), // No computations
	CALC(60), // Calculate column
	LINK(70), // Link column
	ACCU(90), // Accumulate column
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static DefinitionType fromInt(int value) {
	    for (DefinitionType kind : DefinitionType.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return DefinitionType.NONE;
	 }

	private DefinitionType(int value) {
		this.value = value;
	}
}
