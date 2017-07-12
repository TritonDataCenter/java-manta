package com.joyent.manta.exception;

public class MantaReflectionException extends MantaException {
    public MantaReflectionException(String msg) {
        super(msg);
    }

    public MantaReflectionException(String msg, ReflectiveOperationException e) {
        super(msg, e);
    }
}
