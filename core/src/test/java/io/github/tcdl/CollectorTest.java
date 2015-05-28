package io.github.tcdl;

import static io.github.tcdl.events.Event.MESSAGE_EVENT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class CollectorTest {

    private static final String TOPIC = "test:collector";
    private static Message originaMessageWithPayload = TestUtils.createMsbRequestMessageWithPayloadAndTopicTo(TOPIC);
    private static Message originaMessageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(TOPIC);

    @Mock
    private MessageFactory messageFactoryMock;

    @Mock
    private MsbMessageOptions messageOptionsMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private MsbConfigurations msbConfigurationsMock;

    @Before
    public void setUp() throws IOException {

    }

    @Test(expected = NullPointerException.class)
    public void testCreateCollectorNullMsgOptions() {
       new Collector(null, channelManagerMock, msbConfigurationsMock);
    }

    @Test
    public void testGetWaitForResponsesConfigsNoWaitForResponsesReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        assertFalse("expect false if MessageOptions.waitForResponses null", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnFalse() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(-1);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        assertFalse("expect false if MessageOptions.waitForResponses equals -1", collector.isWaitForResponses());
    }

    @Test
    public void testGetWaitForResponsesConfigsReturnTrue() {
        when(messageOptionsMock.getWaitForResponses()).thenReturn(100);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        assertTrue("expect true if MessageOptions.waitForResponses equals 100", collector.isWaitForResponses());
    }

    @Test
    public void testIsAwaitingAcksConfigsNotSetAckTimeoutReturnFalse() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(null);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        assertFalse("expect false if MessageOptions.ackTimeout null", collector.isAwaitingAcks());
    }

    @Test
    public void testIsAwaitingAcksReturnTrue() {
        when(messageOptionsMock.getAckTimeout()).thenReturn(200);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        assertTrue("expect true if MessageOptions.ackTimeout equals 200", collector.isAwaitingAcks());
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterNull() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch eventsFired = awaitRecievePayloadEvents();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnTrue() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        Predicate<Message> filterMock = mockFilterResult(true);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecievePayloadEvents();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        assertTrue(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesFilterReturnFalse() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        Predicate<Message> filterMock = mockFilterResult(false);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecievePayloadEvents();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        verify(filterMock).test(eq(originaMessageWithPayload));
        assertFalse(collector.getPayloadMessages().contains(originaMessageWithPayload));
        assertFalse(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesWaitForOneMoreResponseNoTimeout() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 2
        */
        int ackAndTimeoutMs = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackAndTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(ackAndTimeoutMs);
        when(messageOptionsMock.getWaitForResponses()).thenReturn(2);        
      
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        //give extra time for processing scheduled end() task after ack timeout
        assertFalse(endEventFired.await(ackAndTimeoutMs + 100, TimeUnit.MILLISECONDS));        
    }
    
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievePayloadAwaitAck() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 1
        */
        int ackimeoutMs = 200;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackimeoutMs);       
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);        
      
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);

        //give extra time for processing scheduled end() task after ack timeout
        assertTrue(endEventFired.await(ackimeoutMs + 100, TimeUnit.MILLISECONDS));        
    }
    
    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponseTimeoutAckTaskMoreReponsesNoTimeout() throws InterruptedException {
        /*ackTimeout = 200, responseTimeout=200; waitForResponses = 1
        */
        int ackimeoutMs = 200;      
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackimeoutMs);       
        when(messageOptionsMock.getWaitForResponses()).thenReturn(1);     
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
      
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithPayload);
        
        //send ack message with responsesRemaining between ackTimeout task scheduled and run 
        onMessageCaptur.getValue().onEvent(messageWithAck);
       
        //give extra time for processing scheduled end() task after ack timeout
        assertFalse(endEventFired.await(ackimeoutMs + 100, TimeUnit.MILLISECONDS));        
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAck() throws InterruptedException {
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        Predicate<Message> filterMock = mockFilterResult(true);
        collector.listenForResponses(TOPIC, filterMock);
        CountDownLatch eventsFired = awaitRecieveAckEvents();

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(originaMessageWithAck);

        verify(filterMock).test(eq(originaMessageWithAck));
        assertTrue(collector.getAckMessages().contains(originaMessageWithAck));
        assertTrue(eventsFired.await(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutRunAckTimeoutTask() throws InterruptedException {
        //verify that ackTimeout will fire END event if no more responses remaining 
        int timeoutMs = 3000;
        int ackTimeoutMs = 250;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(3000);
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(0).setTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();
      

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageWithAck);      

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(ackTimeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void testListenForResponsesRecievedAckWithTimeoutRunTimeoutTask() throws InterruptedException {
        //verify that timeout will fire END event if  more responses remaining 
        int timeoutMs = 250;
        int ackTimeoutMs = 3000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(0);
        Acknowledge ack = new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).setTimeoutMs(timeoutMs).build();
        Message messageWithAck = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                ack, TOPIC);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();
      
        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageWithAck);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testListenForResponsesRecievedAckWithTimeoutResponsesRemaining() throws InterruptedException {
        int timeoutMs = 300;
        int ackTimeoutMs = 5000;
        when(messageOptionsMock.getAckTimeout()).thenReturn(ackTimeoutMs);
        Collector collector = new Collector(messageOptionsMock, channelManagerMock, msbConfigurationsMock);
        collector.listenForResponses(TOPIC, null);
        CountDownLatch endEventFired = awaitRecieveEndEvent();
        Message messageSetResponsesRemaining = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(
                new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId()).setResponsesRemaining(1).setTimeoutMs(timeoutMs).build(), TOPIC);

        ArgumentCaptor<SingleArgEventHandler> onMessageCaptur = ArgumentCaptor.forClass(SingleArgEventHandler.class);
        verify(channelManagerMock).on(eq(MESSAGE_EVENT), onMessageCaptur.capture());
        onMessageCaptur.getValue().onEvent(messageSetResponsesRemaining);

        //give extra time for processing scheduled end() task
        assertTrue(endEventFired.await(timeoutMs + 100, TimeUnit.MILLISECONDS));
        verify(channelManagerMock).removeConsumer(TOPIC);
    }

    private CountDownLatch awaitRecievePayloadEvents() {
        CountDownLatch eventsExpectedOnPayload = new CountDownLatch(2);
        when(channelManagerMock.emit(eq(Event.RESPONSE_EVENT), any(Payload.class))).thenAnswer(invocation -> {
            eventsExpectedOnPayload.countDown();
            return channelManagerMock;
        });
        when(channelManagerMock.emit(eq(Event.PAYLOAD_EVENT), any(Payload.class))).thenAnswer(invocation -> {
            eventsExpectedOnPayload.countDown();
            return channelManagerMock;
        });
        return eventsExpectedOnPayload;
    }

    private CountDownLatch awaitRecieveAckEvents() {
        CountDownLatch eventExpectedOnAck = new CountDownLatch(1);
        when(channelManagerMock.emit(eq(Event.ACKNOWLEDGE_EVENT), any(Payload.class))).thenAnswer(invocation -> {
            eventExpectedOnAck.countDown();
            return channelManagerMock;
        });
        return eventExpectedOnAck;
    }

    private CountDownLatch awaitRecieveEndEvent() {
        CountDownLatch eventEndOnAck = new CountDownLatch(1);
        when(channelManagerMock.emit(eq(Event.END_EVENT), any())).thenAnswer(invocation -> {
            eventEndOnAck.countDown();
            return channelManagerMock;
        });
        return eventEndOnAck;
    }

    @SuppressWarnings("unchecked")
    private Predicate<Message> mockFilterResult(boolean result) {
        Predicate<Message> filterMock = mock(Predicate.class);
        when(filterMock.test(any(Message.class))).thenReturn(result);
        return filterMock;
    }
}
