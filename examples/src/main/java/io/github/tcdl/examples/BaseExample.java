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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class BaseExample {

    private MsbContext context = new MsbContext.MsbContextBuilder().
            withShutdownHook(true).
            build();

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
        sendRequest(requester, "REQUEST", true, waitForResponses, null, responseCallback);
    }

    public void sendRequest(Requester requester, String bodyText, Integer waitForResponses, Callback<Payload> responseCallback) throws Exception {
        sendRequest(requester, bodyText, true, waitForResponses, null, responseCallback);
    }


    public void sendRequest(Requester requester, String bodyText,  boolean waitForAck, Integer waitForResponses,
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

        requester.publish(createPayloadWithBodyText(bodyText));
    }

    public ResponderServer createResponderServer(String namespace, ResponderServer.RequestHandler requestHandler) {
        MessageTemplate options = new MessageTemplate();
        System.out.println(">>> RESPONDER SERVER on: " + namespace);
        return ResponderServer.create(namespace, options, context, requestHandler);
    }

    public void respond(Responder responder) {
        responder.send(createPayloadWithBodyText("RESPONSE"));
    }

    public void respond(Responder responder, String text) {
        responder.send(createPayloadWithBodyText(text));
    }

    public void sleep(int timeout) throws InterruptedException {
        Thread.sleep(timeout);
    }

    public void shutdown() {
        context.shutdown();
    }

    public Payload createPayloadWithBodyText(String text) {
        Map<String, String> body = new HashMap<>();
        body.put("text", text);
        return new Payload.PayloadBuilder().setBody(body).build();
    }
}
