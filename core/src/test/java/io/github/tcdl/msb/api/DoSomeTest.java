package io.github.tcdl.msb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.message.payload.Body;
import io.github.tcdl.msb.message.payload.MyPayload;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by ruslan on 20.07.15.
 */
public class DoSomeTest {

    /**
     * Holds amount of time in milliseconds necessary for a message to be transferred from requester to responder.
     * Necessary for time-related assertions.
     * You may consider adjusting these values for fast/slow testing environments.
     */
    private static final int MESSAGE_TRANSMISSION_TIME = 5000;
    private static final int MESSAGE_ROUNDTRIP_TRANSMISSION_TIME = MESSAGE_TRANSMISSION_TIME * 2;

    private MsbContextImpl msbContext;

    @Before
    public void setUp() throws Exception {
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testMe() throws Exception {
        String namespace = "test:my-new";
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate();
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withResponseTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
                .withWaitForResponses(0)
                .build();

        CountDownLatch respSend = new CountDownLatch(1);
        CountDownLatch respReceived = new CountDownLatch(1);

        ConcurrentLinkedQueue<Payload> sentResponses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Payload> receivedResponses = new ConcurrentLinkedQueue<>();

        //Create and send request message directly to broker, wait for response
        Body body = new Body();
        body.setBody("HELO MSB");
        MyPayload requestPayload = new MyPayload();
        requestPayload.setBody(body);


        msbContext.getObjectFactory().createRequester(namespace, requestOptions)
                //.onResponse(payload -> {
                 //   receivedResponses.add(payload);
                 //   respReceived.countDown();
                //})
                .publish(requestPayload);

        //listen for message and send response
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.RequestHandler<MyPayload> handler = (request, response) -> {
            System.out.println("INCOMING MESSAGE >>>" + request.getBody().getBody());
            Payload payload = new Payload.Builder().withBody(
                    new HashMap<String, String>().put("body", "payload from test : testResponderAnswerWithResponseRequesterReceiveResponse"))
                    .withStatusCode(3333).build();
            response.send(payload);
            sentResponses.add(payload);
            respSend.countDown();
        };
        serverMsbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, handler
                , MyPayload.class)
                .listen();

        assertTrue("Message response was not send", respSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message response not received", respReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one response", receivedResponses.size() == 1);
        assertEquals(sentResponses.poll().getStatusCode(), receivedResponses.poll().getStatusCode());
    }

}
