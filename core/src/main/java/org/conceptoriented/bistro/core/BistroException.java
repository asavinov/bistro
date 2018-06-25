package org.conceptoriented.bistro.core;

public class BistroException extends RuntimeException {
    public BistroErrorCode code;
    public String message;
    public String description;

    public Exception e;

    @Override
    public String toString() {
        return "[" + this.code + "]: " + this.message;
    }

    public BistroException(BistroErrorCode code, String message, String description, Exception e) {
        this(code, message, description);
        this.e = e;
    }
    public BistroException(BistroErrorCode code, String message, String description) {
        this.code = code;
        this.message = message;
        this.description = description;
    }
}
