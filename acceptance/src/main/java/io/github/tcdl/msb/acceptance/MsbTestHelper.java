package io.github.tcdl.msb.acceptance;

import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

/**
 * Utility to simplify using requester and responder server
 */
public class MsbTestHelper {

    public static final String DEFAULT_CONTEXT_NAME = "DEFAULT_CONTEXT";

    private static MsbTestHelper instance;

    private Map<String, MsbContext> contextMap = new HashMap<>();

    private MsbTestHelper() {
    }

    public static MsbTestHelper getInstance() {
        if (instance == null) {
            instance = new MsbTestHelper();
        }
        return instance;
    }

    public void initDefault() {
        contextMap.put(DEFAULT_CONTEXT_NAME, new MsbContextBuilder()
                .enableShutdownHook(true).build());
    }

    public void initWithConfig(Config config) {
        initWithConfig(DEFAULT_CONTEXT_NAME, config);
    }

    public void initWithConfig(String contextName, Config config) {
        contextMap.put(contextName, new MsbContextBuilder()
                .withConfig(config)
                .enableShutdownHook(true).build());
    }

    public MsbContext getContext(String contextName) {
        return contextMap.get(contextName);
    }

    public MsbContext getDefaultContext() {
        return getContext(DEFAULT_CONTEXT_NAME);
    }

    public ObjectMapper getPayloadMapper(String contextName) {
        return ((MsbContextImpl) getContext(contextName)).getPayloadMapper();
    }

    public ObjectMapper getPayloadMapper() {
        return getPayloadMapper(DEFAULT_CONTEXT_NAME);
    }

    public <T> Requester<T> createRequester(String namespace, Integer numberOfResponses, Class<T> responsePayloadClass) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, numberOfResponses, null, null, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String contextName, String namespace, Integer numberOfResponses, Class<T> responsePayloadClass) {
        return createRequester(contextName, namespace, numberOfResponses, null, null, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout, Class<T> responsePayloadClass) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, numberOfResponses, ackTimeout, responseTimeout, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String contextName, String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout, Class<T> responsePayloadClass) {
        RequestOptions options = new RequestOptions.Builder()
                .withWaitForResponses(numberOfResponses)
                .withAckTimeout(Utils.ifNull(ackTimeout, 5000))
                .withResponseTimeout(Utils.ifNull(responseTimeout, 15000))
                .build();
        return getContext(contextName).getObjectFactory().createRequester(namespace, options, responsePayloadClass);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, Integer waitForResponses, Callback<T> responseCallback) throws Exception {
        sendRequest(requester, payload, true, waitForResponses, null, responseCallback, null);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, Integer waitForResponses, Callback<T> responseCallback, Callback<Void> endCallback, String tag) throws Exception {
        sendRequest(requester, payload, true, waitForResponses, null, responseCallback, endCallback, tag);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, Integer waitForResponses, Callback<T> responseCallback, Callback<Void> endCallback) throws Exception {
        sendRequest(requester, payload, true, waitForResponses, null, responseCallback, endCallback);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback, Callback<T> responseCallback) throws Exception {
        sendRequest(requester, payload, waitForAck, waitForResponses,
                 ackCallback, responseCallback, null);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback, Callback<T> responseCallback, Callback<Void> endCallback) throws Exception {
        sendRequest(requester, payload, waitForAck, waitForResponses, ackCallback, responseCallback, endCallback, null);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback, Callback<T> responseCallback, Callback<Void> endCallback, String tag) throws Exception {

        requester.onAcknowledge((acknowledge, ackHandler) -> {
            System.out.println(">>> ACKNOWLEDGE: " + acknowledge);
            if (waitForAck && ackCallback != null)
                ackCallback.call(acknowledge);
        });

        requester.onResponse((response, ackHandler) -> {
            System.out.println(">>> RESPONSE: " + response);
            if (waitForResponses != null && waitForResponses > 0 && responseCallback != null) {
                responseCallback.call(response);
            }
        });

        requester.onEnd((end) -> {
            System.out.println(">>> END: ");
            if(endCallback != null) {
                endCallback.call(null);
            }
        });

        requester.publish(payload, tag);
    }

    public ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<RestPayload> requestHandler) {
        return createResponderServer(DEFAULT_CONTEXT_NAME, namespace, requestHandler);
    }

    public ResponderServer createResponderServer(String contextName, String namespace, ResponderServer.RequestHandler<RestPayload> requestHandler) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getContext(contextName).getObjectFactory().createResponderServer(namespace, options, requestHandler, RestPayload.class);
    }

    public <T> ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getDefaultContext().getObjectFactory().createResponderServer(namespace, options, requestHandler, payloadClass);
    }

    public void shutdown(String contextName) {
        MsbContext context = getContext(contextName);
        if (context != null) {
            context.shutdown();
            contextMap.remove(contextName);
        }
    }

    public void shutdown() {
        getDefaultContext().shutdown();
    }

    public RestPayload<?, ?, ?, ?> createFacetParserPayload(String query, String body) {
        Map<String, String> queryMap = new HashMap<>();
        queryMap.put("q", query);

        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("body", body);
        return new RestPayload.Builder<Map<String, String>, Object, Object, Map<String, String>>()
                .withQuery(queryMap)
                .withBody(bodyMap)
                .build();
    }
}