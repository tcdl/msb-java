package io.github.tcdl.msb.api;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * {@link PayloadConverter} utility class to convert objects to instances of given class
 */
public interface PayloadConverter {
    /**
     * Convenience method that allows to specify target type as {@link Class}
     */
    <T> T getAs(Object source, Class<T> destClass);

    /**
     * Converts object to instance of type
     *
     * @param source object to convert
     * @param destTypeReference type to convert to
     * @return instance of class clazz
     */
    <T> T getAs(Object source, TypeReference<T> destTypeReference);
}
