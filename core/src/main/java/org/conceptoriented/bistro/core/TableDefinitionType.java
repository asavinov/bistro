package org.conceptoriented.bistro.core;

public enum TableDefinitionType {
	NOOP(0), // No definition
	PROD(10), // Product table
	;

	private int value;

	public int getValue() {
		return value;
	}

	public static TableDefinitionType fromInt(int value) {
	    for (TableDefinitionType kind : TableDefinitionType.values()) {
	        if (kind.getValue() == value) {
	            return kind;
	        }
	    }
	    return TableDefinitionType.NOOP;
	 }

	private TableDefinitionType(int value) {
		this.value = value;
	}
}
