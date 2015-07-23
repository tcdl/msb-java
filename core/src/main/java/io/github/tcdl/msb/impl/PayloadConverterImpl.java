package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.support.Utils;

/**
 * Implementation of {@link PayloadConverter}
 */
public class PayloadConverterImpl implements PayloadConverter {

    private ObjectMapper messageMapper;

    protected PayloadConverterImpl(ObjectMapper messageMapper) {
        this.messageMapper = messageMapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(Object obj, Class<T> clazz) {
        return Utils.fromJson(Utils.toJson(obj, messageMapper), clazz, messageMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(Object obj, TypeReference<T> typeReference) {
        return Utils.toCustomTypeReference(obj, typeReference, messageMapper);
    }
}
