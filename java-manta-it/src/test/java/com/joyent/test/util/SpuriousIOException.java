package com.joyent.test.util;

import java.io.IOException;

public class SpuriousIOException extends IOException {

    SpuriousIOException(String message) {
        super(message);
    }
}
