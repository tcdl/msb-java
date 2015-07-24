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

import java.util.Map;

/**
 * Utility to simplify using requester and responder server
 */
public class MsbTestHelper {

    private static MsbTestHelper instance;

    private MsbContext context;

    private MsbTestHelper() {
    }

    public static MsbTestHelper getInstance() {
        if (instance == null) {
            instance = new MsbTestHelper();
        }
        return instance;
    }

    public void initDefault() {
        context = new MsbContextBuilder()
                .enableShutdownHook(true).build();
    }

    public void initWithConfig(Config config) {
        context = new MsbContextBuilder()
                .withConfig(config)
                .enableShutdownHook(true).build();
    }

    public MsbContext getContext() {
        return context;
    }

    public ObjectMapper getObjectMapper() {
        return ((MsbContextImpl) (getContext())).getPayloadMapper();
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String namespace, Integer numberOfResponses) {
        return createRequester(namespace, numberOfResponses, null, null);
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout) {
        return createRequester(namespace, numberOfResponses, ackTimeout, null);
    }

    public Requester<Payload<Object, Object, Object, Map<String, Object>>> createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout) {
        RequestOptions options = new RequestOptions.Builder()
                .withWaitForResponses(numberOfResponses)
                .withAckTimeout(Utils.ifNull(ackTimeout, 5000))
                .withResponseTimeout(Utils.ifNull(responseTimeout, 15000))
                .build();
        return context.getObjectFactory().createRequester(namespace, options, new TypeReference<Payload<Object, Object, Object, Map<String, Object>>>() {});
    }

    public void sendRequest(Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, Integer waitForResponses, Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {
        sendRequest(requester, "QUERY", null, true, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, String body, Integer waitForResponses, Callback<Payload<Object, Object, Object, Map<String, Object>>> responseCallback) throws Exception {
        sendRequest(requester, null, body, false, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester<Payload<Object, Object, Object, Map<String, Object>>> requester, String query, String body, boolean waitForAck, Integer waitForResponses,
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

        requester.publish(createPayload(query, body));
    }

    public ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<Payload> requestHandler) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return context.getObjectFactory().createResponderServer(namespace, options, requestHandler);
    }

    public <T extends Payload> ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler<T> requestHandler, Class<T> payloadClass) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return context.getObjectFactory().createResponderServer(namespace, options, requestHandler, payloadClass);
    }

    public void respond(Responder responder) {
        responder.send(createPayload(null, "RESPONSE"));
    }

    public void respond(Responder responder, String text) {
        responder.send(createPayload(null, text));
    }

    public void sleep(int timeout) throws InterruptedException {
        Thread.sleep(timeout);
    }

    public void shutdown() {
        context.shutdown();
    }

    public Payload createPayload(String query, String body) {
        ObjectMapper mapper = ((MsbContextImpl) context).getPayloadMapper();
        return new Payload.Builder<Map, Object, Object, Map>()
                .withQuery(Utils.fromJson("{\"q\": \"" + query + "\"}", Map.class, mapper))
                .withBody(Utils.fromJson("{\"body\": \"" + body + "\"}", Map.class, mapper))
                .build();
    }
}