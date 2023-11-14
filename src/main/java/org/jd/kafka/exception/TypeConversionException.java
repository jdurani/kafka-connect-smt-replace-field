package org.jd.kafka.exception;

import org.apache.kafka.connect.errors.ConnectException;

public class TypeConversionException extends ConnectException {

    public TypeConversionException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
