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

			case NAME_RESOLUTION_ERROR:
				message = "Name not found.";
				break;
			case ELEMENT_CREATION_ERROR:
				message = "Error creating an element.";
				break;
			case ELEMENT_UPDATE_ERROR:
				message = "Error updating an element.";
				break;
			case ELEMENT_DELETE_ERROR:
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
