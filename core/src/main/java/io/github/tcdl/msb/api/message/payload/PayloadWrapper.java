package io.github.tcdl.msb.api.message.payload;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.exception.JsonConversionException;
import io.github.tcdl.msb.api.exception.MessageBuilderException;
import io.github.tcdl.msb.support.Utils;

/**
 * Wraps payload to provide possibilities to convert payload parts
 */
public class PayloadWrapper extends Payload {

    private ObjectMapper mapper;

    private PayloadWrapper(Payload payload, ObjectMapper mapper) {
        setStatusCode(payload.getStatusCode());
        setStatusMessage(payload.getStatusMessage());
        setHeaders(payload.getHeaders());
        setQuery(payload.getQuery());
        setParams(payload.getParams());
        setBody(payload.getBody());
        setBodyBuffer(payload.getBodyBuffer());
        this.mapper = mapper;
    }

    public static Payload wrap(Payload payload, ObjectMapper mapper) {
        return new PayloadWrapper(payload, mapper);
    }

    public <T> T getHeadersAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(getHeaders(), mapper), clazz, mapper);
        } catch (JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getQueryAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(getQuery(), mapper), clazz, mapper);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getParamsAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(getParams(), mapper), clazz, mapper);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getBodyAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(getBody(), mapper), clazz, mapper);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }
}
