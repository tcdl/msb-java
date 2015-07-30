package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.support.Utils;

/**
 * Implementation of {@link PayloadConverter}
 */
public class PayloadConverterImpl implements PayloadConverter {

    private ObjectMapper payloadMapper;

    protected PayloadConverterImpl(ObjectMapper payloadMapper) {
        this.payloadMapper = payloadMapper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(Object obj, Class<T> clazz) {
        return Utils.convert(obj, clazz, payloadMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(Object obj, TypeReference<T> typeReference) {
        return Utils.convert(obj, typeReference, payloadMapper);
    }
}
