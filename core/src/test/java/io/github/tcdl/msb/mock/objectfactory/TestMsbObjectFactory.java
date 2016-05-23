package io.github.tcdl.msb.mock.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

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
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass) {
        return createRequesterForSingleResponse(namespace, payloadClass, 100);
    }

    @Override
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, Class<T> payloadClass, int timeout) {
        return createRequesterForSingleResponse(namespace, null, payloadClass, timeout);
    }

    @Override
    public <T> Requester<T> createRequesterForSingleResponse(String namespace, MessageTemplate messageTemplate, Class<T> payloadClass, int timeout) {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withMessageTemplate(messageTemplate)
                .withWaitForResponses(1)
                .withResponseTimeout(timeout)
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
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, null, payloadTypeReference, null);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, errorHandler, payloadTypeReference, null);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<JsonNode> requestHandler) {
        ResponderCapture<JsonNode> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, null, null, null);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, null, null, payloadClass);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, Class<T> payloadClass) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, errorHandler, null, payloadClass);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> Requester<T> createRequesterForFireAndForget(String namespace) {
        return createRequesterForFireAndForget(namespace, null);
    }

    @Override
    public <T> Requester<T> createRequesterForFireAndForget(String namespace, MessageTemplate messageTemplate) {
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withWaitForResponses(0)
                .build();
        return createRequester(namespace, requestOptions, (Class<T>)null);
    }

    @Override
    public PayloadConverter getPayloadConverter() {
        return null;
    }

    @Override
    public ChannelMonitorAggregator createChannelMonitorAggregator(Callback<AggregatorStats> aggregatorStatsHandler) {
        return null;
    }

    @Override
    public void shutdown() {

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
