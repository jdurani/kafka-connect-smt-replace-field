package org.jd.kafka.exception;

import org.apache.kafka.connect.errors.ConnectException;

public class UnsupportedTargetTypeException extends ConnectException {

    public UnsupportedTargetTypeException(String s) {
        super(s);
    }
}
