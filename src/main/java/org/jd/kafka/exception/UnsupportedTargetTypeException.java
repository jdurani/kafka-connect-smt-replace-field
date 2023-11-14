package org.jd.kafka.exception;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Exception to indicate, that conversion to target type is not supported
 */
public class UnsupportedTargetTypeException extends ConnectException {

    /**
     * Create new instance with more details.
     *
     * @param s details
     */
    public UnsupportedTargetTypeException(String s) {
        super(s);
    }
}
