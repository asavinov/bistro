package org.conceptoriented.bistro.core;

// TODO: add generic error which means no classification and the cause is stored as system level exception object in the corresponding field
public enum BistroErrorCode {
	NONE(0), 
	GENERAL(1), 
	NOT_FOUND_IDENTITY(10), 

	GET_ELEMENT(21), CREATE_ELEMENT(22), UPATE_ELEMENT(23), DELETE_ELEMENT(24),

	DEPENDENCY_CYCLE_ERROR(40),

	TRANSLATE_ERROR(50), PARSE_ERROR(51), BIND_ERROR(52), BUILD_ERROR(53),
	TRANSLATE_PROPAGATION_ERROR(60), PARSE_PROPAGATION_ERROR(61), BIND_PROPAGATION_ERROR(62), BUILD_PROPAGATION_ERROR(63),

	EVALUATE_ERROR(70),
	EVALUATE_PROPAGATION_ERROR(80),
	;

	private int value;

	public int getValue() {
		return value;
	}

	private BistroErrorCode(int value) {
		this.value = value;
	}
}
