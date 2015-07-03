package io.github.tcdl.examples;

import io.github.tcdl.Callback;
import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.Responder;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.config.RequestOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;

import java.util.Map;

/**
 * Utility to simplify using requester and responder server
 */
public class MSBUtil {

    private static MSBUtil instance;

    static {
        instance = new MSBUtil();
    }

    private MsbContext context;

    private MSBUtil() {
    }

    public static MSBUtil getInstance() {
        instance.init();
        return instance;
    }

    public void init() {
        context = new MsbContext.MsbContextBuilder().withShutdownHook(true).build();
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

        return Requester.create(namespace, options, context);
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
        return ResponderServer.create(namespace, options, context, requestHandler);
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
