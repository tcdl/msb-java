package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.EventHandlers;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class RequesterTest {

    @Mock
    private MsbMessageOptions messageOptionsMock;

    @Mock
    private EventHandlers eventHandlerMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Producer producerMock;

    @Mock
    private Consumer consumerMock;

    @Mock
    private Consumer.Subscriber subscriberMock;

    @Mock
    private Collector collectorMock;

    @Test
    public void testPublishNoWaitForResponses() throws Exception {
        Requester requester = initRequesterForResponsesWithTimeout(0);

        requester.publish(TestUtils.createSimpleRequestPayload());

        verify(collectorMock, never()).listenForResponses(anyString(), any());
        verify(collectorMock).end();
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        Requester requester = initRequesterForResponsesWithTimeout(1);

        requester.publish(TestUtils.createSimpleRequestPayload());

        verify(collectorMock).listenForResponses(anyString(), any(Predicate.class));
        verify(collectorMock, never()).end();
    }

    @Test
    public void testPublishCallProducerPublishWithPayload() throws Exception {
        Requester requester = initRequesterForResponsesWithTimeout(0);
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Payload payload = TestUtils.createSimpleRequestPayload();

        requester.publish(payload);

        verify(producerMock).publish(messageCaptor.capture());
        assertThat(payload, is(messageCaptor.getValue().getPayload()));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAcknowledgeEventHandlerIsAdded() throws Exception {
        Callback onAckMock = mock(Callback.class);
        Requester requester = initRequesterForResponsesWithTimeout(1);

        requester.onAcknowledge(onAckMock);

        assertThat(requester.eventHandlers.onAcknowledge(), is(onAckMock));
        assertThat(requester.eventHandlers.onResponse(), not(onAckMock));
        assertThat(requester.eventHandlers.onError(), not(onAckMock));
        assertThat(requester.eventHandlers.onEnd(), not(onAckMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResponseEventHandlerIsAdded() throws Exception {
        Callback onResponseMock = mock(Callback.class);
        Requester requester = initRequesterForResponsesWithTimeout(1);

        requester.onResponse(onResponseMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onResponseMock));
        assertThat(requester.eventHandlers.onResponse(), is(onResponseMock));
        assertThat(requester.eventHandlers.onError(), not(onResponseMock));
        assertThat(requester.eventHandlers.onEnd(), not(onResponseMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testErrorEventHandlerIsAdded() throws Exception {
        Callback onErrorMock = mock(Callback.class);
        Requester requester = initRequesterForResponsesWithTimeout(1);

        requester.onError(onErrorMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onErrorMock));
        assertThat(requester.eventHandlers.onResponse(), not(onErrorMock));
        assertThat(requester.eventHandlers.onError(), is(onErrorMock));
        assertThat(requester.eventHandlers.onEnd(), not(onErrorMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEndEventHandlerIsAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        Requester requester = initRequesterForResponsesWithTimeout(1);

        requester.onEnd(onEndMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onError(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), is(onEndMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNoEventHandlerAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        Requester requester = initRequesterForResponsesWithTimeout(1);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onError(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), not(onEndMock));
    }

    @Test
    public void testRequestMessage() throws Exception {
        Requester requester = initRequesterForResponsesWithTimeout(0);
        Payload request = TestUtils.createSimpleRequestPayload();

        requester.publish(request);

        Message requestMessage = requester.getMessage();
        assertNotNull(requestMessage);
        assertNotNull(requestMessage.getMeta());
        assertNotNull(requestMessage.getPayload());
    }

    private Requester initRequesterForResponsesWithTimeout(int numberOfResponses) throws Exception {
        MsbMessageOptions messageOptionsMock = mock(MsbMessageOptions.class);
        when(messageOptionsMock.getNamespace()).thenReturn("test:requester");
        when(messageOptionsMock.getWaitForResponses()).thenReturn(numberOfResponses);
        when(messageOptionsMock.getResponseTimeout()).thenReturn(100);

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);
        when(channelManagerMock.subscribe(anyString(), subscriberMock)).thenReturn(consumerMock);

        MsbContext msbContext = TestUtils.createSimpleMsbContext();
        msbContext.setChannelManager(channelManagerMock);

        Requester requester = spy(Requester.create(messageOptionsMock, null, msbContext));

        collectorMock = spy(new Collector(messageOptionsMock, msbContext, eventHandlerMock));

        doReturn(collectorMock)
                .when(requester)
                .createCollector(any(MsbMessageOptions.class), any(MsbContext.class), any(EventHandlers.class));

        return requester;
    }

}
