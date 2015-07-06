package io.github.tcdl.api;

import io.github.tcdl.MsbContextImpl;
import io.github.tcdl.impl.RequesterImpl;
import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.api.message.Acknowledge;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.impl.ResponderServerImpl;
import io.github.tcdl.support.TestUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RequesterResponderIT {

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
    public void testResponderServerReceiveMessageSendByRequester() throws Exception {
        String namespace = "test:requester-responder-test-request-received";
        RequestOptions requestOptions = TestUtils.createSimpleRequestOptions();
        CountDownLatch requestReceived = new CountDownLatch(1);

        //Create and send request message
        RequesterImpl requester = RequesterImpl.create(namespace, requestOptions, msbContext);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        ResponderServerImpl.create(namespace, requestOptions.getMessageTemplate(), msbContext, (request, response) -> {
            requestReceived.countDown();
        })
                .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not received", requestReceived.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testResponderAnswerWithAckRequesterReceiveAck() throws Exception {
        String namespace = "test:requester-responder-test-get-ack";
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate();
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withAckTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
                .withWaitForResponses(1)
                .build();

        CountDownLatch ackSend = new CountDownLatch(1);
        CountDownLatch ackResponseReceived = new CountDownLatch(1);

        List<Acknowledge> receivedResponseAcks = new LinkedList<>();

        //Create and send request message directly to broker, wait for ack
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        RequesterImpl.create(namespace, requestOptions, msbContext).
                onAcknowledge((Acknowledge ack) -> {
                    receivedResponseAcks.add(ack);
                    ackResponseReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send ack
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServerImpl.create(namespace, messageTemplate, serverMsbContext, (request, response) -> {
            response.sendAck(100, 2);
            ackSend.countDown();
        })
                .listen();

        assertTrue("Message ack was not send", ackSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not received", ackResponseReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", receivedResponseAcks.size() == 1);
    }

    @Test
    public void testResponderAnswerWithResponseRequesterReceiveResponse() throws Exception {
        String namespace = "test:requester-responder-test-get-resp";
        MessageTemplate messageTemplate = TestUtils.createSimpleMessageTemplate();
        RequestOptions requestOptions = new RequestOptions.Builder()
            .withResponseTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
            .withWaitForResponses(1)
            .build();

        CountDownLatch respSend = new CountDownLatch(1);
        CountDownLatch respReceived = new CountDownLatch(1);

        ConcurrentLinkedQueue<Payload> sentResponses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Payload> receivedResponses = new ConcurrentLinkedQueue<>();

        //Create and send request message directly to broker, wait for response
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        RequesterImpl.create(namespace, requestOptions, msbContext)
                .onResponse(payload -> {
                    receivedResponses.add(payload);
                    respReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send response
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServerImpl
                .create(namespace, messageTemplate, serverMsbContext, (request, response) -> {
                    Payload payload = new Payload.PayloadBuilder().withBody(
                            new HashMap<String, String>().put("body", "payload from test : testResponderAnswerWithResponseRequesterReceiveResponse"))
                            .withStatusCode(3333).build();
                    response.send(payload);
                    sentResponses.add(payload);
                    respSend.countDown();

                })
                .listen();

        assertTrue("Message response was not send", respSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message response not received", respReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one response", receivedResponses.size() == 1);
        assertEquals(sentResponses.poll().getStatusCode(), receivedResponses.poll().getStatusCode());
    }

    @Test
    public void testMultipleRequesterListenForAcks() throws Exception {
        String namespace = "test:requester-responder-test-send-multiple-requests-get-ack";
        RequestOptions requestOptions = new RequestOptions.Builder()
                .withAckTimeout(100)
                .withResponseTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
                .withWaitForResponses(1)
                .build();

        int requestsToSendDuringTest = 5;

        CountDownLatch ackSend = new CountDownLatch(requestsToSendDuringTest);
        CountDownLatch ackResponseReceived = new CountDownLatch(requestsToSendDuringTest);

        Set<Acknowledge> receivedResponseAcks = new HashSet<>();

        //Create and send request messages directly to broker, wait for ack
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        final AtomicInteger messagesToSend = new AtomicInteger(requestsToSendDuringTest);
        Thread publishingThread = new Thread(() -> {
            while (messagesToSend.get() > 0) {

                RequesterImpl.create(namespace, requestOptions, msbContext).
                        onAcknowledge((Acknowledge ack) -> {
                            receivedResponseAcks.add(ack);
                            ackResponseReceived.countDown();
                        })
                        .publish(requestPayload);
                messagesToSend.decrementAndGet();
            }

        });
        publishingThread.setDaemon(true);
        publishingThread.start();

        //listen for message and send ack
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        Random randomAckValue = new Random();
        randomAckValue.ints();
        ResponderServerImpl.create(namespace, requestOptions.getMessageTemplate(), serverMsbContext, (request, response) -> {
            response.sendAck(randomAckValue.nextInt(), randomAckValue.nextInt());
            ackSend.countDown();
        })
                .listen();

        assertTrue("Message ack was not send", ackSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not received", ackResponseReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", receivedResponseAcks.size() == requestsToSendDuringTest);
    }

    @Test
    public void testResponderCommunicationWithAck () throws Exception {
        String namespace1 = "test:requester-responder-server-one";
        String namespace2 = "test:requester-responder-server-two";
        MessageTemplate responderServerOneMessageOptions = TestUtils.createSimpleMessageTemplate();
        MessageTemplate responderServerTwoMessageOptions = TestUtils.createSimpleMessageTemplate();

        RequestOptions requestAwaitAckMessageOptions = new RequestOptions.Builder()
            .withAckTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
            .withWaitForResponses(1)
            .build();

        CountDownLatch ackSent = new CountDownLatch(1);
        CountDownLatch ackReceived = new CountDownLatch(1);

        MsbContextImpl serverOneMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServerImpl.create(namespace1, responderServerOneMessageOptions, serverOneMsbContext, (request, response) -> {

            //Create and send request message, wait for ack
            RequesterImpl requester = RequesterImpl.create(namespace2, requestAwaitAckMessageOptions, msbContext);
            Payload requestPayload = TestUtils.createSimpleRequestPayload();
            requester.onAcknowledge((Acknowledge a) -> ackReceived.countDown());
            requester.publish(requestPayload);
        })
                .listen();

        MsbContextImpl serverTwoMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServerImpl.create(namespace2, responderServerTwoMessageOptions, serverTwoMsbContext, (request, response) -> {
            response.sendAck(100, 2);
            ackSent.countDown();
        })
                .listen();

        MockAdapter.pushRequestMessage(TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(namespace1));

        assertTrue("Message ack was not send", ackSent.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message ack was not received", ackReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }
}
