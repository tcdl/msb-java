package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import io.github.tcdl.support.Utils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class RequesterTest {

    private MsbContext msbContext;
    private MsbMessageOptions messageOptions;

    @Mock
    private ChannelManager channelManagerMock;
    private Collector collectorMock;
    @Mock
    private Producer producerMock;
    @Mock
    private Consumer consumerMock;

    private Requester requester;

    public void initRequesterForResponses(int numberOfResponses) throws Exception {
        initRequesterForResponses(numberOfResponses, null);
    }

    public void initRequesterForResponses(int numberOfResponses, EventHandlers eventHandlers) throws Exception {
        messageOptions = TestUtils.createSimpleConfig();
        messageOptions.setWaitForResponses(numberOfResponses);

        msbContext = TestUtils.createSimpleMsbContext();
        msbContext.setChannelManager(channelManagerMock);

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);
        when(channelManagerMock.findOrCreateConsumer(anyString())).thenReturn(consumerMock);


        EventHandlers handlers = Utils.ifNull(eventHandlers, new EventHandlers());
        collectorMock = spy(new Collector(messageOptions, msbContext, handlers));

        requester = new Requester(messageOptions, null, msbContext) {
            protected Collector createCollector(MsbMessageOptions messageOptions, MsbContext channelManager, EventHandlers eventHandlers) {
                return collectorMock;
            }
        };
    }

    @After
    public void tearDown() {
        reset(channelManagerMock, producerMock);
    }

    @Test
    public void testPublishNoWaitForResponses() throws Exception {
        initRequesterForResponses(0);

        Payload request = TestUtils.createSimpleRequestPayload();
        requester.publish(request);

        verify(collectorMock, never()).listenForResponses(anyString(), any());

        assertRequest(request);
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        initRequesterForResponses(1);

        Payload request = TestUtils.createSimpleRequestPayload();
        requester.publish(request);

        verify(collectorMock).listenForResponses(anyString(), any());

        assertRequest(request);
    }

    @Test
    public void testOnLastMessageEnd() throws Exception {
        initRequesterForResponses(0);

        Message message = TestUtils.createMsbResponseMessage();
        collectorMock.handleMessage(message);

        verify(collectorMock).end();
    }

    @Test
    public void testOnRemainingSetTimeout() throws Exception {
        initRequesterForResponses(1);

        Message message = TestUtils.createMsbResponseMessage();
        requester.publish(TestUtils.createSimpleRequestPayload());
        collectorMock.handleMessage(message);

        verify(collectorMock).waitForResponses();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnAcknowledge() throws Exception {
        Callback onAckMock = mock(Callback.class);
        initRequesterForResponses(1, new EventHandlers().onAcknowledge(onAckMock));

        Message message = TestUtils.createMsbRequestMessageWithAckNoPayloadAndTopicTo(messageOptions.getNamespace());
        requester.onError(onAckMock);
        requester.publish(message.getPayload());
        collectorMock.handleMessage(message);

        verify(onAckMock).call(eq(message.getAck()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnResponse() throws Exception {
        Callback onResponseMock = mock(Callback.class);
        initRequesterForResponses(1, new EventHandlers().onResponse(onResponseMock));

        Message message = TestUtils.createMsbResponseMessage();
        requester.onResponse(onResponseMock);
        requester.publish(message.getPayload());
        collectorMock.handleMessage(message);

        verify(onResponseMock).call(eq(message.getPayload()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnError() throws Exception {
        Callback onErrorMock = mock(Callback.class);
        initRequesterForResponses(1, new EventHandlers().onError(onErrorMock));

        Message message = TestUtils.createMsbResponseMessage();
        Exception exception = new Exception();
        requester.onError(onErrorMock);
        requester.publish(message.getPayload());
        collectorMock.handleError(exception);

        verify(onErrorMock).call(eq(exception));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnEnd() throws Exception {
        Callback onEndMock = mock(Callback.class);
        initRequesterForResponses(1, new EventHandlers().onEnd(onEndMock));

        Message message = TestUtils.createMsbResponseMessage();
        requester.onEnd(onEndMock);
        requester.publish(message.getPayload());
        collectorMock.handleMessage(message);

        verify(onEndMock).call(anyListOf(Message.class));
    }

    @Test
    public void testRequestMessage() throws Exception {
        initRequesterForResponses(0);

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
        verify(producer).publish(messageCaptor.capture());

        Message message = messageCaptor.getValue();
        assertNotNull(message.getPayload());

        return message;
    }
}
