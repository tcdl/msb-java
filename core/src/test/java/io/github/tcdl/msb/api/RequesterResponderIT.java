package io.github.tcdl.msb.api;

import io.github.tcdl.msb.adapters.mock.MockAdapter;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.impl.MsbContextImpl;
import io.github.tcdl.msb.message.payload.Body;
import io.github.tcdl.msb.message.payload.MyPayload;
import io.github.tcdl.msb.support.TestUtils;
import io.github.tcdl.msb.support.Utils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    public static final int MESSAGE_TRANSMISSION_TIME = 5000;
    public static final int MESSAGE_ROUNDTRIP_TRANSMISSION_TIME = MESSAGE_TRANSMISSION_TIME * 2;

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
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(namespace, requestOptions);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        msbContext.getObjectFactory().createResponderServer(namespace, requestOptions.getMessageTemplate(), (request, response) -> {
            requestReceived.countDown();
        })
                .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not received", requestReceived.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testResponderServerReceiveCustomPayloadMessageSendByRequester() throws Exception {
        String namespace = "test:requester-responder-test-custom-request-received";
        RequestOptions requestOptions = TestUtils.createSimpleRequestOptions();
        CountDownLatch requestReceived = new CountDownLatch(1);

        //Create and send request message
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(namespace, requestOptions);
        Body sentBody = new Body("test:requester-responder-test-body");
        Payload<Object, Object, Object, Body> requestPayload = new Payload.Builder<Object, Object, Object, Body>()
                .withBody(sentBody)
                .build();

        Body receivedBody = new Body();
        msbContext.getObjectFactory().createResponderServer(namespace, requestOptions.getMessageTemplate(), (request, response) -> {
            receivedBody.setBody(request.getBody().getBody());
            requestReceived.countDown();
        }, MyPayload.class)
                .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not received", requestReceived.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertEquals(receivedBody, sentBody);
    }

    @Test
    public void testResponderServerReceiveCustomPayloadMessageSendByRequesterAndConvertBody() throws Exception {
        String namespace = "test:requester-responder-test-custom-request-received-and-converted";
        RequestOptions requestOptions = TestUtils.createSimpleRequestOptions();
        CountDownLatch requestReceived = new CountDownLatch(1);

        //Create and send request message
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(namespace, requestOptions);
        Body sentBody = new Body("test:requester-responder-test-body");
        Payload<Object, Object, Object, Body> requestPayload = new Payload.Builder<Object, Object, Object, Body>()
                .withBody(sentBody)
                .build();

        Body receivedBody = new Body();
        msbContext.getObjectFactory().createResponderServer(namespace, requestOptions.getMessageTemplate(), (request, response) -> {
            PayloadConverter payloadConverter = msbContext.getObjectFactory().getPayloadConverter();
            receivedBody.setBody(payloadConverter.getAs(request.getBody(), Body.class).getBody());
            requestReceived.countDown();
        })
        .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not received", requestReceived.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertEquals(receivedBody, sentBody);
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
        msbContext.getObjectFactory().createRequester(namespace, requestOptions).
                onAcknowledge((Acknowledge ack) -> {
                    receivedResponseAcks.add(ack);
                    ackResponseReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send ack
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        serverMsbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, (request, response) -> {
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
        msbContext.getObjectFactory().createRequester(namespace, requestOptions, Payload.class)
                .onResponse(payload -> {
                    receivedResponses.add(payload);
                    respReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send response
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("body", "payload from test testResponderAnswerWithResponseRequesterReceiveResponse");
        serverMsbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, (request, response) -> {
            Payload payload = new Payload.Builder<Object, Object, Object, Map<String, String>>()
                    .withBody(bodyMap)
                    .withStatusCode(3333)
                    .build();
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
    public void testResponderAnswerWithResponseRequesterReceiveCustomPayloadResponse() throws Exception {
        String namespace = "test:requester-responder-test-get-custom-resp";
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
        Body receivedBody = new Body();
        msbContext.getObjectFactory().createRequester(namespace, requestOptions, MyPayload.class)
                .onResponse(payload -> {
                    receivedResponses.add(payload);
                    receivedBody.setBody(payload.getBody().getBody());
                    respReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send response
        MsbContextImpl serverMsbContext = TestUtils.createSimpleMsbContext();
        Body responseBody = new Body("payload from test testResponderAnswerWithResponseRequesterReceiveResponse");
        Payload responsePayload = new Payload.Builder<Object, Object, Object, Body>()
                .withBody(responseBody)
                .build();

        serverMsbContext.getObjectFactory().createResponderServer(namespace, messageTemplate, (request, response) -> {
            response.send(responsePayload);
            sentResponses.add(responsePayload);
            respSend.countDown();
        })
                .listen();

        assertTrue("Message response was not send", respSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message response not received", respReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one response", receivedResponses.size() == 1);
        assertEquals(sentResponses.poll().getStatusCode(), receivedResponses.poll().getStatusCode());
        assertEquals(responseBody, receivedBody);
    }

    @Test
    public void testResponderCommunicationWithAck() throws Exception {
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
        serverOneMsbContext.getObjectFactory().createResponderServer(namespace1, responderServerOneMessageOptions, (request, response) -> {

            //Create and send request message, wait for ack
            Requester<Payload> requester = msbContext.getObjectFactory().createRequester(namespace2, requestAwaitAckMessageOptions);
            Payload requestPayload = TestUtils.createSimpleRequestPayload();
            requester.onAcknowledge((Acknowledge a) -> ackReceived.countDown());
            requester.publish(requestPayload);
        })
                .listen();

        MsbContextImpl serverTwoMsbContext = TestUtils.createSimpleMsbContext();
        serverTwoMsbContext.getObjectFactory().createResponderServer(namespace2, responderServerTwoMessageOptions, (request, response) -> {
            response.sendAck(100, 2);
            ackSent.countDown();
        })
                .listen();

        MockAdapter.pushRequestMessage(namespace1,
                Utils.toJson(TestUtils.createMsbRequestMessageWithSimplePayload(namespace1), msbContext.getPayloadMapper()));

        assertTrue("Message ack was not send", ackSent.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message ack was not received", ackReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
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

                msbContext.getObjectFactory().createRequester(namespace, requestOptions).
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
        serverMsbContext.getObjectFactory().createResponderServer(namespace, requestOptions.getMessageTemplate(), (request, response) -> {
            response.sendAck(randomAckValue.nextInt(), randomAckValue.nextInt());
            ackSend.countDown();
        })
                .listen();

        assertTrue("Message ack was not send", ackSend.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not received", ackResponseReceived.await(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", receivedResponseAcks.size() == requestsToSendDuringTest);
    }

    @Test
    public void testRequestMessageCollectorUnsubscribeAfterResponsesAndSubscribeAgain() throws Exception {
        String namespace = "test:requester-message-collector-resubscribe-onmessage";
        RequestOptions requestOptionsWaitResponse = new RequestOptions.Builder()
                .withMessageTemplate(new MessageTemplate())
                .withResponseTimeout(MESSAGE_ROUNDTRIP_TRANSMISSION_TIME)
                .withWaitForResponses(1)
                .build();

        Thread serverListenThread = new Thread(() -> {
            msbContext.getObjectFactory().createResponderServer(namespace, requestOptionsWaitResponse.getMessageTemplate(), (request, response) -> {
                Payload payload = new Payload.Builder<Object, Object, Object, String>()
                        .withBody(new HashMap<String, String>()
                                .put("body", "payload from test : testRequestMessageCollectorUnsubscribeAfterResponsesAndSubscribeAgain"))
                        .withStatusCode(4444)
                        .build();
                response.send(payload);
            })
                    .listen();

        });
        serverListenThread.setDaemon(true);
        serverListenThread.start();

        CountDownLatch endConversation1 = new CountDownLatch(1);
        CountDownLatch endConversation2 = new CountDownLatch(1);

        //Create and send request message
        Requester<Payload> requester = msbContext.getObjectFactory().createRequester(namespace, requestOptionsWaitResponse).onEnd(arg -> endConversation1.countDown());
        requester.publish(TestUtils.createSimpleRequestPayload());
        assertTrue("Message was not received", endConversation1.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));

        //Create and send request message
        requester.onEnd(arg -> endConversation2.countDown());
        requester.publish(TestUtils.createSimpleRequestPayload());
        assertTrue("Message was not received", endConversation2.await(MESSAGE_TRANSMISSION_TIME, TimeUnit.MILLISECONDS));
    }
}
