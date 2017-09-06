package org.conceptoriented.bistro.core;

public enum ColumnKind {
	NONE(0), // No computations
	CALC(60), // Compute
	LINK(70), // Link
	ACCU(90), // Accumulate
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
	    return ColumnKind.NONE;
	 }

	private ColumnKind(int value) {
		this.value = value;
	}
}
