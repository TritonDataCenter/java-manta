/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

/**
 * @author Yunong Xiao
 */
public class MantaException extends Exception {

        private static final long serialVersionUID = -2045814978953401214L;

        public MantaException() {
        }

        /**
         * @param message
         */
        public MantaException(String message) {
                super(message);
        }

        /**
         * @param cause
         */
        public MantaException(Throwable cause) {
                super(cause);
        }

        /**
         * @param message
         * @param cause
         */
        public MantaException(String message, Throwable cause) {
                super(message, cause);
        }

        /**
         * @param message
         * @param cause
         * @param enableSuppression
         * @param writableStackTrace
         */
        public MantaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
                super(message, cause, enableSuppression, writableStackTrace);
        }

}
