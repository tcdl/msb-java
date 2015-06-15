package io.github.tcdl.examples;

import io.github.tcdl.MsbContext;
import io.github.tcdl.Requester;
import io.github.tcdl.Responder;
import io.github.tcdl.ResponderServer;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by rdrozdov-tc on 6/15/15.
 */
public class BaseAcceptanceTest {

    public MsbContext createContext() {
        return new MsbContext.MsbContextBuilder().build();
    }

    public Requester createRequester(MsbContext context, String namespace, Integer numberOfResponses) {
        return createRequester(context, namespace, numberOfResponses, null, null);
    }

    public Requester createRequester(MsbContext context, String namespace, Integer numberOfResponses, Integer ackTimeout) {
        return createRequester(context, namespace, numberOfResponses, ackTimeout, null);
    }

    public Requester createRequester(MsbContext context, String namespace, Integer numberOfResponses, Integer ackTimeout, Integer responseTimeout) {
        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace(namespace);
        options.setWaitForResponses(numberOfResponses);
        options.setAckTimeout(Utils.ifNull(ackTimeout, 3000));
        options.setResponseTimeout(Utils.ifNull(responseTimeout, 10000));

        return Requester.create(options, context);
    }

    public void sendRequest(Requester requester, boolean waitForAck, Integer waitForResponses) throws Exception {
        List<CountDownLatch> awaitList = new ArrayList<>();

        if (waitForAck) {
            CountDownLatch awaitAck = new CountDownLatch(1);
            requester.onAcknowledge(acknowledge -> awaitAck.countDown());
            awaitList.add(awaitAck);
        }

        if (waitForResponses != null && waitForResponses > 0) {
            CountDownLatch awaitResponses = new CountDownLatch(waitForResponses);
            requester.onResponse(response -> awaitResponses.countDown());
            awaitList.add(awaitResponses);
        }

        requester.publish(createPayloadWithBodyText("REQUEST"));

        for (CountDownLatch await : awaitList) {
            await.await(10, TimeUnit.SECONDS);
            assertEquals(0, await.getCount());
        }
    }

    public ResponderServer createResponderServer(MsbContext context, String namespace) {
        MsbMessageOptions options = new MsbMessageOptions();
        options.setNamespace(namespace);
        return ResponderServer.create(options, context);
    }

    public void respond(Responder responder) {
        responder.send(createPayloadWithBodyText("RESPONSE"));
    }

    public Payload createPayloadWithBodyText(String text) {
        Map<String, String> body = new HashMap<>();
        body.put("text", text);
        return new Payload.PayloadBuilder().setBody(body).build();
    }
}
