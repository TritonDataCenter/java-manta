/**
 * 
 */
package com.joyent.manta.exception;

/**
 * @author yunong
 * 
 */
public class MantaCryptoException extends MantaClientException {

        private static final long serialVersionUID = -5734849034194919231L;

        public MantaCryptoException() {
        }

        /**
         * @param message
         */
        public MantaCryptoException(String message) {
                super(message);
        }

        /**
         * @param cause
         */
        public MantaCryptoException(Throwable cause) {
                super(cause);
        }

        /**
         * @param message
         * @param cause
         */
        public MantaCryptoException(String message, Throwable cause) {
                super(message, cause);
        }

        /**
         * @param message
         * @param cause
         * @param enableSuppression
         * @param writableStackTrace
         */
        public MantaCryptoException(String message, Throwable cause, boolean enableSuppression,
                                    boolean writableStackTrace) {
                super(message, cause, enableSuppression, writableStackTrace);
        }

}
