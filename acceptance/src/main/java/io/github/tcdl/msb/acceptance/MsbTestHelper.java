package io.github.tcdl.msb.acceptance;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.MsbContext;
import io.github.tcdl.msb.api.MsbContextBuilder;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;

import java.util.HashMap;
import java.util.Map;

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

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String namespace, Integer numberOfResponses) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, numberOfResponses, null, null);
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String contextName, String namespace, Integer numberOfResponses) {
        return createRequester(contextName, namespace, numberOfResponses, null, null);
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, numberOfResponses, ackTimeout, responseTimeout);
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String contextName, String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout) {
        RequestOptions options = new RequestOptions.Builder()
                .withWaitForResponses(numberOfResponses)
                .withAckTimeout(Utils.ifNull(ackTimeout, 5000))
                .withResponseTimeout(Utils.ifNull(responseTimeout, 15000))
                .build();
        return getContext(contextName).getObjectFactory().createRequester(namespace, options,
                new TypeReference<Payload<Object, Object, Object, Map<String, Object>>>() {
                });
    }

    public void sendRequest(Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, Integer waitForResponses, Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {
        sendRequest(DEFAULT_CONTEXT_NAME, requester, "QUERY", null, true, waitForResponses, null, responseCallback);
    }

    public void sendRequest(String contextName, Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, Integer waitForResponses, Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {
        sendRequest(contextName, requester, "QUERY", null, true, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, String query, String body, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback,
            Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {

        sendRequest(DEFAULT_CONTEXT_NAME, requester, query, body, waitForAck, waitForResponses, ackCallback, responseCallback);
    }

    public void sendRequest(String contextName, Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, String query, String body, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback,
            Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {

        requester.onAcknowledge(acknowledge -> {
            System.out.println(">>> ACKNOWLEDGE: " + acknowledge);
            if (waitForAck && ackCallback != null)
                ackCallback.call(acknowledge);
        });

        requester.onResponse(response -> {
            System.out.println(">>> RESPONSE: " + response);
            if (waitForResponses != null && waitForResponses > 0 && responseCallback != null) {
                responseCallback.call(response);
            }
        });

        requester.publish(createPayload(contextName, query, body));
    }

    public ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<Payload> requestHandler) {
        return createResponderServer(DEFAULT_CONTEXT_NAME, namespace, requestHandler);
    }

    public ResponderServer createResponderServer(String contextName, String namespace, ResponderServer.RequestHandler<Payload> requestHandler) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getContext(contextName).getObjectFactory().createResponderServer(namespace, options, requestHandler);
    }

    public <T extends Payload> ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getDefaultContext().getObjectFactory().createResponderServer(namespace, options, requestHandler, payloadClass);
    }

    public void respond(Responder responder) {
        responder.send(createPayload(MsbTestHelper.DEFAULT_CONTEXT_NAME, null, "RESPONSE"));
    }

    public void respond(Responder responder, String text) {
        responder.send(createPayload(MsbTestHelper.DEFAULT_CONTEXT_NAME, null, text));
    }

    public void sleep(int timeout) throws InterruptedException {
        Thread.sleep(timeout);
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

    public Payload createPayload(String contextName, String query, String body) {
        ObjectMapper mapper = ((MsbContextImpl) getContext(contextName)).getPayloadMapper();
        return new Payload.Builder<Map, Object, Object, Map>()
                .withQuery(Utils.fromJson("{\"q\": \"" + query + "\"}", Map.class, mapper))
                .withBody(Utils.fromJson("{\"body\": \"" + body + "\"}", Map.class, mapper))
                .build();
    }
}