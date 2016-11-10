package io.github.tcdl.msb.mock.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.tcdl.msb.api.*;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.Type;

/**
 * {@link ObjectFactory} implementation that captures all requesters/responders params and callbacks to be
 * used during testing.
 */
public class TestMsbObjectFactory implements ObjectFactory {

    private final TestMsbStorageForObjectFactory storage = new TestMsbStorageForObjectFactory();

    public TestMsbStorageForObjectFactory getStorage() {
        return storage;
    }

    @Override
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference) {
        RequesterCapture<T> capture = new RequesterCapture<>(namespace, requestOptions, payloadTypeReference, null);
        storage.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass, RequestOptions baseRequestOptions) {
        Validate.notNull(baseRequestOptions);
        RequestOptions requestOptions = baseRequestOptions
                .asBuilder()
                .withWaitForResponses(1)
                .withAckTimeout(0)
                .build();

        return createRequester(namespace, requestOptions, toTypeReference(payloadClass));
    }

    @Override
    public Requester<JsonNode> createRequester(String namespace, RequestOptions requestOptions) {
        RequesterCapture<JsonNode> capture = new RequesterCapture<>(namespace, requestOptions, null, null);
        storage.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, Class<T> payloadClass) {
        RequesterCapture<T> capture = new RequesterCapture<>(namespace, requestOptions, null, payloadClass);
        storage.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, ResponderOptions responderOptions, ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, responderOptions.getBindingKeys(), responderOptions.getMessageTemplate(), requestHandler, null, payloadTypeReference, null);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> Requester<T> createRequesterForFireAndForget(String namespace, RequestOptions requestOptions) {
        Validate.notNull(requestOptions);
        RequestOptions fireAndForgetRequestOptions = requestOptions.asBuilder().withWaitForResponses(0).build();

        return createRequester(namespace, fireAndForgetRequestOptions, (Class<T>) null);
    }

    @Override
    public PayloadConverter getPayloadConverter() {
        return null;
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
