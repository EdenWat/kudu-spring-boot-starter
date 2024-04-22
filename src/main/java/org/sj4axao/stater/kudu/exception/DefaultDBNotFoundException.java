package org.sj4axao.stater.kudu.exception;

public class DefaultDBNotFoundException extends RuntimeException {
    public DefaultDBNotFoundException() {
    }

    public DefaultDBNotFoundException(String message) {
        super(message);
    }
}
