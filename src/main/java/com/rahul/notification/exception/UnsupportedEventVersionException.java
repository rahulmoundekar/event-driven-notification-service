package com.rahul.notification.exception;

public class UnsupportedEventVersionException
        extends RuntimeException {

    public UnsupportedEventVersionException(
            String message) {

        super(message);
    }
}