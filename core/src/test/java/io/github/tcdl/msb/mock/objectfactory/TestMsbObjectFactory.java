package io.github.tcdl.msb.mock.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ObjectFactory;
import io.github.tcdl.msb.api.PayloadConverter;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

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

    @Override public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, ResponderServer.ErrorHandler errorHandler, Class<T> payloadClass) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, errorHandler, null, payloadClass);
        storage.addCapture(capture);
        return capture.getResponderServerMock();
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

}
