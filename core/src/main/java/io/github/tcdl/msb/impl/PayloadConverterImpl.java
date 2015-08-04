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
    public <T> T getAs(Object source, Class<T> destClass) {
        return Utils.convert(source, destClass, payloadMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAs(Object source, TypeReference<T> destTypeReference) {
        return Utils.convert(source, destTypeReference, payloadMapper);
    }
}
