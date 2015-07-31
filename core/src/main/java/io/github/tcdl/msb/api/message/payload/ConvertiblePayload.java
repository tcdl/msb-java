package io.github.tcdl.msb.api.message.payload;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Declares methods to convert payload parts to different types
 */
public interface ConvertiblePayload {

    default <T> T getHeadersAs(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    default <T> T getQueryAs(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    default <T> T getParamsAs(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    default <T> T getBodyAs(Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    default <T> T getBodyAs(TypeReference<T> typeReference) {
        throw new UnsupportedOperationException();
    }
}
