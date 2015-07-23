package io.github.tcdl.msb.api;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * {@link PayloadConverter} utility class to convert objects to instances of given class
 */
public interface PayloadConverter {
    /**
     * Converts object to instance of given class
     *
     * @param obj object to convert
     * @param clazz class to convert to
     * @return instance of class clazz
     */
    <T> T getAs(Object obj, Class<T> clazz);

    /**
     * Converts object to instance of type
     *
     * @param obj object to convert
     * @param typeReference type to convert to
     * @return instance of class clazz
     */
    <T> T getAs(Object obj, TypeReference<T> typeReference);
}
