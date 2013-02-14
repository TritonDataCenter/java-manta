/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaObjectException extends MantaException {

        private static final long serialVersionUID = 6799144229159150444L;

        public MantaObjectException() {
        }

        /**
         * @param message
         */
        public MantaObjectException(String message) {
                super(message);
        }

        /**
         * @param cause
         */
        public MantaObjectException(Throwable cause) {
                super(cause);
        }

        /**
         * @param message
         * @param cause
         */
        public MantaObjectException(String message, Throwable cause) {
                super(message, cause);
        }

}
