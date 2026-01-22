package com.pms.transactional.exceptions;

public class SystemFailureException extends Exception {

    public SystemFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
