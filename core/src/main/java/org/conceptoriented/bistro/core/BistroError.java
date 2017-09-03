package org.conceptoriented.bistro.core;

import org.json.JSONObject;

// TODO: add field referencing embedded exception which caused this (domain specific) exception
public class BistroError extends Exception {
	public BistroErrorCode code;
	public String message;
	public String description;
	
	public String toJson() {
		String jcode = "`code`:" + this.code.getValue() + "";
		String jmessage = "`message`: " + JSONObject.valueToString(this.message) + "";
		String jdescription = "`description`: " + JSONObject.valueToString(this.description) + "";

		String json = jcode + ", " + jmessage + ", " + jdescription;

		return ("{" + json + "}").replace('`', '"');
	}

	public static String error(BistroErrorCode code, String message2) {
		String message = "";
		switch(code) {

			case NOT_FOUND_IDENTITY:
				message = "Identity not found. Session expired. Enable cookies. Reload page.";
				break;
			case GET_ELEMENT:
				message = "Error getting an element.";
				break;
			case CREATE_ELEMENT:
				message = "Error creating an element.";
				break;
			case UPATE_ELEMENT:
				message = "Error updating an element.";
				break;
			case DELETE_ELEMENT:
				message = "Error deleting an element.";
				break;
			default:
				message = "Unknown error";
				break;
		}
		
		BistroError error = new BistroError(code, message, message2);

		return "{\"error\": " + error.toJson() + "}";
	}

	public static String error(BistroErrorCode code, String message, String message2) {
		BistroError error = new BistroError(code, message, message2);
		return "{\"error\": " + error.toJson() + "}";
	}

	@Override
	public String toString() {
		return "[" + this.code + "]: " + this.message;
	}
	
	public BistroError(BistroErrorCode code, String message, String description) {
		this.code = code;
		this.message = message;
		this.description = description;
	}
}
