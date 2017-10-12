package com.joyent.test.util;

import java.io.IOException;

public class SpuriousIOException extends IOException {

    private static final long serialVersionUID = 229727410007992086L;

    SpuriousIOException(String message) {
        super(message);
    }
}
