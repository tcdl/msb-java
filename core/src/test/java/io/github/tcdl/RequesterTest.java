package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Requester.class)
public class RequesterTest {

    private MsbContext msbContext;
    private MsbMessageOptions messageOptions;

    @Mock
    private MsbMessageOptions messageOptionsMock;
    
    @Mock
    private ChannelManager channelManagerMock;
    private Collector collectorMock;
    @Mock
    private Producer producerMock;
    @Mock
    private Consumer consumerMock;

    public void initMocks(MsbMessageOptions config) throws Exception {
        msbContext = TestUtils.createSimpleMsbContext();
        msbContext.setChannelManager(channelManagerMock);

        collectorMock = spy(new Collector(config, msbContext.getChannelManager(), msbContext.getMsbConfig()));
        PowerMockito.whenNew(Collector.class).withAnyArguments().thenReturn(collectorMock);

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);
        when(channelManagerMock.findOrCreateConsumer(anyString())).thenReturn(consumerMock);
        when(channelManagerMock.findConsumer(anyString())).thenReturn(consumerMock);
    }

    public void initForResponses(int numberOfResponses) throws Exception {
        messageOptions = TestUtils.createSimpleConfig();
        messageOptions.setWaitForResponses(numberOfResponses);
        initMocks(messageOptions);
    }

    @After
    public void tearDown() {
        reset(channelManagerMock, producerMock, consumerMock);
    }

    @Test
    public void testPublishNoWaitForResponses() throws Exception {
        initForResponses(0);

        Payload request = TestUtils.createSimpleRequestPayload();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.publish(request);

        verify(collectorMock, never()).listenForResponses(anyString(), any());

        assertRequest(request);
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        initForResponses(1);

        Payload request = TestUtils.createSimpleRequestPayload();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.publish(request);

        verify(collectorMock).listenForResponses(anyString(), any());

        assertRequest(request);
    }

    @Test
    public void testOnLastMessageEnd() throws Exception {
        initForResponses(0);

        Message message = TestUtils.createMsbResponseMessage();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.handleMessage(message, null);

        verify(collectorMock).end();
    }

    @Test
    public void testOnRemainingSetTimeout() throws Exception {
        initForResponses(1);

        Message message = TestUtils.createMsbResponseMessage();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.handleMessage(message, null);

        verify(collectorMock).enableTimeout();
    }

    @Test
    public void testOnErrorFiresError() throws Exception {
        initForResponses(1);

        Message message = TestUtils.createMsbResponseMessage();
        Exception exception = new Exception();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.publish(message.getPayload());
        requester.handleMessage(message, exception);

        verify(consumerMock).emit(eq(Event.ERROR_EVENT), eq(exception));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubscribeOnAcknowledge() throws Exception {
        initForResponses(1);

        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.onAcknowledge(acknowledge -> {
        });

        verify(consumerMock).on(eq(Event.ACKNOWLEDGE_EVENT), any(SingleArgEventHandler.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubscribeOnResponse() throws Exception {
        initForResponses(1);

        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.onResponse(response -> {
        });

        verify(consumerMock).on(eq(Event.RESPONSE_EVENT), any(SingleArgEventHandler.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubscribeOnError() throws Exception {
        initForResponses(1);

        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.onError(error -> {
        });

        verify(consumerMock).on(eq(Event.ERROR_EVENT), any(SingleArgEventHandler.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSubscribeOnEnd() throws Exception {
        initForResponses(1);

        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.onEnd(args -> {
        });

        verify(consumerMock).on(eq(Event.END_EVENT), any(SingleArgEventHandler.class));
    }

    @Test
    public void testIsMessageAcknowledged() throws Exception {
        initForResponses(1);

        Requester requester = new Requester(messageOptions, null, msbContext);
        when(collectorMock.getAckMessages())
                .thenReturn(Arrays.asList(TestUtils.createMsbResponseMessage()));

        assertTrue(requester.isMessageAcknowledged());
    }

    @Test
    public void testRequestMessage() throws Exception {
        initForResponses(0);

        Payload request = TestUtils.createSimpleRequestPayload();
        Requester requester = new Requester(messageOptions, null, msbContext);
        requester.publish(request);

        Message requestMessage = requester.getMessage();
        assertNotNull(requestMessage);
        assertNotNull(requestMessage.getMeta());
        assertNotNull(requestMessage.getPayload());
    }

    private void assertRequest(Payload request) {
        Message sentMessage = captureMessage(producerMock);
        assertNotNull(sentMessage.getPayload());
        assertEquals(request, sentMessage.getPayload());
    }

    private Message captureMessage(Producer producer) {
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(producer).publish(messageCaptor.capture(), any());

        Message message = messageCaptor.getValue();
        assertNotNull(message.getPayload());

        return message;
    }
}
