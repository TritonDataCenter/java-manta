package com.joyent.manta.util;

import java.lang.reflect.Field;

/**
 * Abstract class providing reflection helper methods for use with
 * serialization.
 *
 * @param <T> type to clone
 */
public class AbstractCloner<T> {
    /**
     * Class to be serialized.
     */
    private Class<T> classReference;

    /**
     * Creates a new serializer instance for the specified class.
     *
     * @param classReference class to be serialized
     */
    public AbstractCloner(final Class<T> classReference) {
        this.classReference = classReference;
    }

    /**
     * Gets a reference to a field on an object.
     *
     * @param fieldName field to get
     * @return reference to an object's field
     * @throws UnsupportedOperationException when the field isn't present
     */
    protected Field captureField(final String fieldName) {
        final Field field = MantaReflectionUtils.getField(classReference, fieldName);

        if (field == null) {
            String msg = String.format("No field [%s] found on object [%s]", fieldName, classReference);
            throw new UnsupportedOperationException(msg);
        }

        return field;
    }

}
