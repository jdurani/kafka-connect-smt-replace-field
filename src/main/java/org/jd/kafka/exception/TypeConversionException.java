package org.jd.kafka.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Exception to indicate that conversion to target type failed.
 */
public class TypeConversionException extends ConnectException {

    /**
     * Create new instance with detailed message and cause.
     *
     * @param s         message
     * @param throwable cause
     */
    public TypeConversionException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
