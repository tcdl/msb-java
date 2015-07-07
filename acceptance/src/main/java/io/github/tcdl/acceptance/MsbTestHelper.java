package io.github.tcdl.acceptance;

import com.typesafe.config.Config;
import io.github.tcdl.api.Callback;
import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.MsbContextBuilder;
import io.github.tcdl.api.RequestOptions;
import io.github.tcdl.api.Requester;
import io.github.tcdl.api.Responder;
import io.github.tcdl.api.ResponderServer;
import io.github.tcdl.api.message.Acknowledge;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.support.Utils;

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
                .withShutdownHook(true).build();
    }

    public void initWithConfig(Config config) {
        context = new MsbContextBuilder()
                .withConfig(config)
                .withShutdownHook(true).build();
    }

    public MsbContext getContext() {
        return context;
    }

    public Requester createRequester(String namespace, Integer numberOfResponses) {
        return createRequester(namespace, numberOfResponses, null, null);
    }

    public Requester createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout) {
        return createRequester(namespace, numberOfResponses, ackTimeout, null);
    }

    public Requester createRequester(String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout) {
        RequestOptions options = new RequestOptions.Builder()
            .withWaitForResponses(numberOfResponses)
            .withAckTimeout(Utils.ifNull(ackTimeout, 3000))
            .withResponseTimeout(Utils.ifNull(responseTimeout, 10000))
            .build();
        return context.getObjectFactory().createRequester(namespace, options);
    }

    public void sendRequest(Requester requester, Integer waitForResponses, Callback<Payload> responseCallback) throws Exception {
        sendRequest(requester, "QUERY", null, true, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester requester, String body, Integer waitForResponses, Callback<Payload> responseCallback) throws Exception {
        sendRequest(requester, null, body, false, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester requester, String query, String body, boolean waitForAck, Integer waitForResponses,
            Callback<Acknowledge> ackCallback,
            Callback<Payload> responseCallback) throws Exception {

            requester.onAcknowledge(acknowledge -> {
                System.out.println(">>> ACKNOWLEDGE: " + acknowledge);
                if (waitForAck && ackCallback!= null) ackCallback.call(acknowledge);
            });

            requester.onResponse(response -> {
                System.out.println(">>> RESPONSE: " + response);
                if (waitForResponses != null && waitForResponses > 0 && responseCallback != null) {
                    responseCallback.call(response);
                }
            });

        requester.publish(createPayload(query, body));
    }

    public ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler requestHandler) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return context.getObjectFactory().createResponderServer(namespace, options, requestHandler);
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
        return new Payload.PayloadBuilder()
                .withQuery(Utils.fromJson("{\"q\": \"" + query + "\"}", Map.class))
                .withBody(Utils.fromJson("{\"body\": \"" + body + "\"}", Map.class))
                .build();
    }
}
