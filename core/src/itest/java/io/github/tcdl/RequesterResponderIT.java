package io.github.tcdl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequesterResponderIT {

    public static final Logger LOG = LoggerFactory.getLogger(RequesterResponderIT.class);

    private MsbContext msbContext;

    @Before
    public void setUp() throws Exception {
        this.msbContext = TestUtils.createSimpleMsbContext();
    }

    @Test
    public void testResponderServerRecieveMessageSendByRequester() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-testrecieved");
        CountDownLatch requestRecieved = new CountDownLatch(1);

        //Create and send request message
        Requester requester = new Requester(messageOptions, null, msbContext);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        ResponderServer.create(messageOptions, msbContext)
                .use(((request, response) -> {
                    requestRecieved.countDown();
                }))
                .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not recieved", requestRecieved.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testResponderAnswerWithAckRequesterRecieveAck() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-testgetack");
        messageOptions.setAckTimeout(2000);
        messageOptions.setResponseTimeout(2000);
        messageOptions.setWaitForResponses(1);

        CountDownLatch ackSend = new CountDownLatch(1);
        CountDownLatch ackResponseRecieved = new CountDownLatch(1);

        List<Message> sendedAcks = new LinkedList<Message>();
        List<Acknowledge> recievedResponseAcks = new LinkedList<Acknowledge>();

        //Create and send request message directly to broker, wait for ack  
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        new Requester(messageOptions, null, msbContext).
            onAcknowledge((Acknowledge ack) -> {
                recievedResponseAcks.add(ack);
                ackResponseRecieved.countDown();
        })
        .publish(requestPayload);


        //listen for message and send response
        MsbContext serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.create(messageOptions, serverMsbContext)
                .use(((request, response) -> {
                    response.sendAck(100, 2);
                    sendedAcks.add(response.getResponseMessage());
                    ackSend.countDown();
                }))
                .listen();

        assertTrue("Message ack was not send", ackSend.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not recieved", ackResponseRecieved.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", recievedResponseAcks.size() == 1);
        assertEquals(sendedAcks.get(0).getAck().getResponderId(), recievedResponseAcks.get(0).getResponderId());
    }

    @Test
    public void testResponderAnswerWithResponseRecieveResponse() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-testgetresp");
        messageOptions.setAckTimeout(2000);
        messageOptions.setResponseTimeout(3000);
        messageOptions.setWaitForResponses(1);

        CountDownLatch respSend = new CountDownLatch(1);
        CountDownLatch respRecieved = new CountDownLatch(1);

        ConcurrentLinkedQueue<Payload> sendedResponses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Payload> recievedResponses = new ConcurrentLinkedQueue<>();

        //Create and send request message directly to broker, wait for ack  
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        new Requester(messageOptions, null, msbContext)
            .onResponse(payload -> {
                recievedResponses.add(payload);
                respRecieved.countDown();
            })
            .publish(requestPayload);

        //listen for message and send response
        MsbContext serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer
                .create(messageOptions, serverMsbContext)
                .use(((request, response) -> {
                    Payload payload = new Payload.PayloadBuilder().setBody(
                            new HashMap<String, String>().put("body", "payload from test : testResponderAnswerWithResponseRequesterRecieveResponse"))
                            .setStatusCode(3333).build();
                    response.send(payload);
                    sendedResponses.add(payload);
                    respSend.countDown();

                }))
                .listen();

        assertTrue("Message response was not send", respSend.await(2000, TimeUnit.MILLISECONDS));
        assertTrue("Message response not recieved", respRecieved.await(2000, TimeUnit.MILLISECONDS));
        assertTrue("Expected one response", recievedResponses.size() == 1);
        assertEquals(sendedResponses.poll().getStatusCode(), recievedResponses.poll().getStatusCode());
    }

    @Test
    public void testRequesterAwaitAck() throws Exception {
        MsbMessageOptions responderServerOneMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-one");
        MsbMessageOptions responderServerTwoMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-two");

        MsbMessageOptions requestAwaitAckMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-two");
        requestAwaitAckMessageOptions.setAckTimeout(10000);
        requestAwaitAckMessageOptions.setWaitForResponses(1);

        //listen for message and send response
        CountDownLatch ackSent = new CountDownLatch(1);
        CountDownLatch ackRecieved = new CountDownLatch(1);

        MsbContext serverOneMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.create(responderServerOneMessageOptions, serverOneMsbContext)
                .use(((request, response) -> {

                    //Create and send request message, wait for ack 
                    Requester requester = new Requester(requestAwaitAckMessageOptions, null, msbContext);
                    Payload requestPayload = TestUtils.createSimpleRequestPayload();
                    requester.onAcknowledge((Acknowledge a) -> ackRecieved.countDown());
                    requester.publish(requestPayload);
                }))
                .listen();

        MsbContext serverTwoMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.create(responderServerTwoMessageOptions, serverTwoMsbContext)
                .use(((request, response) -> {
                    response.sendAck(100, 2);
                    ackSent.countDown();
                }))
                .listen();

        MockAdapter.pushRequestMessage(TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(responderServerOneMessageOptions.getNamespace()));

        assertTrue("Message ack was not send", ackSent.await(2000, TimeUnit.MILLISECONDS));
        assertTrue("Message ack was not recieved", ackRecieved.await(2000, TimeUnit.MILLISECONDS));
    }
}
