package org.conceptoriented.bistro.core;

// TODO: add generic error which means no classification and the cause is stored as system level exception object in the corresponding field
public enum BistroErrorCode {
	NONE(0), 

	NAME_RESOLUTION_ERROR(10),

	ELEMENT_CREATION_ERROR(20),
	ELEMENT_UPDATE_ERROR(30),
	ELEMENT_DELETE_ERROR(40),

	TRANSLATE_ERROR(50),
	EVALUATE_ERROR(70),

	GENERAL(1000),
	;

	private int value;

	public int getValue() {
		return value;
	}

	private BistroErrorCode(int value) {
		this.value = value;
	}
}
