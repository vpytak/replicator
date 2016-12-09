package com.booking.replication.flusher;

public class FlusherException extends Exception {

    private Exception originalException;

    public FlusherException() {
        this("", null);
    }

    public FlusherException(String message) {
        this(message, null);
    }

    public FlusherException(String message, Exception exception) {
        super(message);
        originalException = exception;
    }

    public Exception getOriginalException() {
        return originalException;
    }
}
