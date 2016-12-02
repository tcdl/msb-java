package io.github.tcdl.msb.acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.support.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

    public MsbContext initDefault() {
        return contextMap.put(DEFAULT_CONTEXT_NAME, new MsbContextBuilder()
                .build());
    }

    public MsbContext initWithConfig(Config config) {
        return initWithConfig(DEFAULT_CONTEXT_NAME, config);
    }

    public MsbContext initWithConfig(String contextName, Config config) {
        MsbContext context = new MsbContextBuilder().withConfig(config).build();
        contextMap.put(contextName, context);
        return context;
    }

    /**
     * Sets exchanges to be non-durable and queues to be temporary. This reduces the chances for tests to affect each other.
     */
    public static Config temporaryInfrastructure(Config config){
        return config.withValue("msbConfig.brokerConfig.durable", ConfigValueFactory.fromAnyRef(false));
    }

    /**
     * Creates MsbContext that will create unique (from context to context) queue names.
     */
    public MsbContext initDistinctContext(Config baseConfig) {
        String uuid = UUID.randomUUID().toString();
        Config config = baseConfig.withValue("msbConfig.serviceDetails.name", ConfigValueFactory.fromAnyRef(uuid));
        return initWithConfig(uuid, config);
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

    public <T> Requester<T> createRequester(String namespace, Integer numberOfResponses, Class<T> responsePayloadClass) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, null, numberOfResponses, null, null, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String contextName, String namespace, String forwardNamespace, Integer numberOfResponses, Class<T> responsePayloadClass) {
        return createRequester(contextName, namespace, forwardNamespace, numberOfResponses, null, null, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout, Class<T> responsePayloadClass) {
        return createRequester(DEFAULT_CONTEXT_NAME, namespace, null, numberOfResponses, ackTimeout, responseTimeout, responsePayloadClass);
    }

    public <T> Requester<T> createRequester(String contextName, String namespace, String forwardNamespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout, Class<T> responsePayloadClass) {
        RequestOptions options = new RequestOptions.Builder()
                .withWaitForResponses(numberOfResponses)
                .withForwardNamespace(forwardNamespace)
                .withAckTimeout(Utils.ifNull(ackTimeout, 5000))
                .withResponseTimeout(Utils.ifNull(responseTimeout, 15000))
                .build();
        return getContext(contextName).getObjectFactory().createRequester(namespace, options, responsePayloadClass);
    }

    public <T> void sendRequest(Requester<T> requester, Object payload, Integer waitForResponses, Callback<T> responseCallback) throws Exception {
        sendRequest(requester, payload, true, waitForResponses, null, responseCallback, null);
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

    public ResponderServer createResponderServer(String contextName, String namespace, ResponderServer.RequestHandler<RestPayload> requestHandler) {
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getContext(contextName).getObjectFactory().createResponderServer(namespace, ResponderOptions.DEFAULTS, requestHandler, RestPayload.class);
    }

    public <T> ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return getDefaultContext().getObjectFactory().createResponderServer(namespace, ResponderOptions.DEFAULTS, requestHandler, payloadClass);
    }

    public void shutdown(String contextName) {
        MsbContext context = getContext(contextName);
        if (context != null) {
            context.shutdown();
            contextMap.remove(contextName);
        }
    }

    public void shutdown() {
        MsbContext context = contextMap.remove(DEFAULT_CONTEXT_NAME);
        if(context != null){
            context.shutdown();
        }
    }

    public void shutdownAll(){
        contextMap.values().forEach(MsbContext::shutdown);
        contextMap.clear();
    }
}