package io.github.tcdl.msb.testsupport.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.monitor.AggregatorStats;
import io.github.tcdl.msb.api.monitor.ChannelMonitorAggregator;

import static org.mockito.Mockito.mock;

/**
 * {@link ObjectFactory} implementation that captures all requesters/responders params and callbacks to be
 * used during testing.
 */
public class TestMsbObjectFactory implements ObjectFactory {

    @Override
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, TypeReference<T> payloadTypeReference) {
        RequesterCapture<T> capture = new RequesterCapture<>(namespace, requestOptions, payloadTypeReference, null);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public Requester<JsonNode> createRequester(String namespace, RequestOptions requestOptions) {
        RequesterCapture<JsonNode> capture = new RequesterCapture<>(namespace, requestOptions, null, null);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public <T> Requester<T> createRequester(String namespace, RequestOptions requestOptions, Class<T> payloadClass) {
        RequesterCapture<T> capture = new RequesterCapture<>(namespace, requestOptions, null, payloadClass);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
        return capture.getRequesterMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler,  payloadTypeReference, null);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<JsonNode> requestHandler) {
        ResponderCapture<JsonNode> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, null, null);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
        return capture.getResponderServerMock();
    }

    @Override
    public <T> ResponderServer createResponderServer(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        ResponderCapture<T> capture = new ResponderCapture<>(namespace, messageTemplate, requestHandler, null, payloadClass);
        TestMsbStorageForObjectFactory.Internal.addCapture(capture);
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
