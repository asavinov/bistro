package org.conceptoriented.bistro.core;

public enum TableDefinitionType {
	NOOP(0), // No definition
	PROJ(10), // Project table column
	PROD(20), // Product column
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
