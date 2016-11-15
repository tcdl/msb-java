package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.*;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.Type;

/**
 * Provides methods for creation {@link Requester} and {@link ResponderServer}.
 */
public class ObjectFactoryImpl implements ObjectFactory {

    private MsbContextImpl msbContext;
    private PayloadConverter payloadConverter;

    public ObjectFactoryImpl(MsbContextImpl msbContext) {
        super();
        this.msbContext = msbContext;
        payloadConverter = new PayloadConverterImpl(msbContext.getPayloadMapper());
    }

    @Override
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference) {
        return RequesterImpl.create(namespace, requestOptions, msbContext, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass, RequestOptions baseRequestOptions) {
        Validate.notNull(baseRequestOptions);
        RequestOptions singleResponseRequestOptions = baseRequestOptions
                .asBuilder()
                .withWaitForResponses(1)
                .build();

        return RequesterImpl.create(namespace, singleResponseRequestOptions, msbContext, toTypeReference(payloadClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions,
                                                     ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        return ResponderServerImpl.create(namespace, responderOptions, msbContext, requestHandler, errorHandler, payloadTypeReference);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Requester<T> createRequesterForFireAndForget(String namespace, RequestOptions requestOptions) {
        Validate.notNull(requestOptions, "RequestOptions are mandatory");

        RequestOptions fireAndForgetRequestOptions = requestOptions
                .asBuilder()
                .withAckTimeout(0)
                .withWaitForResponses(0)
                .build();

        return RequesterImpl.create(namespace, fireAndForgetRequestOptions, msbContext, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PayloadConverter getPayloadConverter() {
        return payloadConverter;
    }

    private static <U> TypeReference<U> toTypeReference(Class<U> clazz) {
        return new TypeReference<U>() {
            @Override
            public Type getType() {
                return clazz;
            }
        };
    }
}
