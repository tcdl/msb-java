package io.github.tcdl;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.tcdl.adapters.mock.MockAdapter;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
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
    public void testResponderServerReceiveMessageSendByRequester() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-test-request-received");
        CountDownLatch requestReceived = new CountDownLatch(1);

        //Create and send request message
        Requester requester = Requester.create(messageOptions, msbContext);
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        ResponderServer.create(messageOptions, msbContext)
                .use(((request, response) -> {
                    requestReceived.countDown();
                }))
                .listen();

        requester.publish(requestPayload);

        assertTrue("Message was not received", requestReceived.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testResponderAnswerWithAckRequesterReceiveAck() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-test-get-ack");
        messageOptions.setAckTimeout(2000);
        messageOptions.setResponseTimeout(2000);
        messageOptions.setWaitForResponses(1);

        CountDownLatch ackSend = new CountDownLatch(1);
        CountDownLatch ackResponseReceived = new CountDownLatch(1);

        List<Message> sentAcks = new LinkedList<Message>();
        List<Acknowledge> receivedResponseAcks = new LinkedList<Acknowledge>();

        //Create and send request message directly to broker, wait for ack  
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester.create(messageOptions, msbContext).
                onAcknowledge((Acknowledge ack) -> {
                    receivedResponseAcks.add(ack);
                    ackResponseReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send ack
        MsbContext serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.create(messageOptions, serverMsbContext)
                .use(((request, response) -> {
                    response.sendAck(100, 2);
                    sentAcks.add(response.getResponseMessage());
                    ackSend.countDown();
                }))
                .listen();

        assertTrue("Message ack was not send", ackSend.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not received", ackResponseReceived.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", receivedResponseAcks.size() == 1);
        assertEquals(sentAcks.get(0).getAck().getResponderId(), receivedResponseAcks.get(0).getResponderId());
    }

    @Test
    public void testResponderAnswerWithResponseRequesterReceiveResponse() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-test-get-resp");
        messageOptions.setAckTimeout(2000);
        messageOptions.setResponseTimeout(3000);
        messageOptions.setWaitForResponses(1);

        CountDownLatch respSend = new CountDownLatch(1);
        CountDownLatch respReceived = new CountDownLatch(1);

        ConcurrentLinkedQueue<Payload> sentResponses = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Payload> receivedResponses = new ConcurrentLinkedQueue<>();

        //Create and send request message directly to broker, wait for response
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester.create(messageOptions, msbContext)
                .onResponse(payload -> {
                    receivedResponses.add(payload);
                    respReceived.countDown();
                })
                .publish(requestPayload);

        //listen for message and send response
        MsbContext serverMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer
                .create(messageOptions, serverMsbContext)
                .use(((request, response) -> {
                    Payload payload = new Payload.PayloadBuilder().setBody(
                            new HashMap<String, String>().put("body", "payload from test : testResponderAnswerWithResponseRequesterReceiveResponse"))
                            .setStatusCode(3333).build();
                    response.send(payload);
                    sentResponses.add(payload);
                    respSend.countDown();

                }))
                .listen();

        assertTrue("Message response was not send", respSend.await(2000, TimeUnit.MILLISECONDS));
        assertTrue("Message response not received", respReceived.await(2000, TimeUnit.MILLISECONDS));
        assertTrue("Expected one response", receivedResponses.size() == 1);
        assertEquals(sentResponses.poll().getStatusCode(), receivedResponses.poll().getStatusCode());
    }

    @Test
    public void testResponderCommunicationWithAck () throws Exception {
        MsbMessageOptions responderServerOneMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-one");
        MsbMessageOptions responderServerTwoMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-two");

        MsbMessageOptions requestAwaitAckMessageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-server-two");
        requestAwaitAckMessageOptions.setAckTimeout(10000);
        requestAwaitAckMessageOptions.setWaitForResponses(1);

        CountDownLatch ackSent = new CountDownLatch(1);
        CountDownLatch ackReceived = new CountDownLatch(1);

        MsbContext serverOneMsbContext = TestUtils.createSimpleMsbContext();
        ResponderServer.create(responderServerOneMessageOptions, serverOneMsbContext)
                .use(((request, response) -> {

                    //Create and send request message, wait for ack 
                    Requester requester = Requester.create(requestAwaitAckMessageOptions, msbContext);
                    Payload requestPayload = TestUtils.createSimpleRequestPayload();
                    requester.onAcknowledge((Acknowledge a) -> ackReceived.countDown());
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
        assertTrue("Message ack was not received", ackReceived.await(2000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testMultipleRequesterListenForAcks() throws Exception {
        MsbMessageOptions messageOptions = TestUtils.createSimpleConfigSetNamespace("test:requester-responder-test-send-multiple-requests-get-ack");
        messageOptions.setAckTimeout(100);
        messageOptions.setResponseTimeout(2000);
        messageOptions.setWaitForResponses(1);

        int requestsToSendDuringTest = 5;

        CountDownLatch ackSend = new CountDownLatch(requestsToSendDuringTest);
        CountDownLatch ackResponseReceived = new CountDownLatch(requestsToSendDuringTest);

        List<Message> sentAcks = new LinkedList<Message>();
        List<Acknowledge> receivedResponseAcks = new LinkedList<Acknowledge>();

        //Create and send request messages directly to broker, wait for ack
        Payload requestPayload = TestUtils.createSimpleRequestPayload();

        final AtomicInteger messagesToSend = new AtomicInteger(requestsToSendDuringTest);
        Thread publishingThread= new Thread(() -> {
            while (messagesToSend.get() > 0) {

                Requester.create(messageOptions, msbContext).
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
        MsbContext serverMsbContext = TestUtils.createSimpleMsbContext();
        Random randomAckValue = new Random();
        randomAckValue.ints();
        ResponderServer.create(messageOptions, serverMsbContext)
                .use(((request, response) -> {
                    response.sendAck(randomAckValue.nextInt(100), randomAckValue.nextInt(100));
                    sentAcks.add(response.getResponseMessage());
                    ackSend.countDown();
                }))
                .listen();

        assertTrue("Message ack was not send", ackSend.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Message ack response not received", ackResponseReceived.await(3000, TimeUnit.MILLISECONDS));
        assertTrue("Expected one ack", receivedResponseAcks.size() == requestsToSendDuringTest);
        assertEquals(sentAcks.stream().map(Message::getAck).collect(toList()), receivedResponseAcks);
    }
}
