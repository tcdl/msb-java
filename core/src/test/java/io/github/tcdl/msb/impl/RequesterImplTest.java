package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Consumer;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.events.EventHandlers;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
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
public class RequesterImplTest {

    private static final String NAMESPACE = "test:requester";

    @Mock
    private EventHandlers<Payload> eventHandlerMock;

    @Mock
    private ChannelManager channelManagerMock;

    @Mock
    private Producer producerMock;

    @Mock
    private Consumer consumerMock;

    @Mock
    private Collector collectorMock;

    @Test
    public void testPublishNoWaitForResponses() throws Exception {
        RequesterImpl<Payload> requester = initRequesterForResponsesWithTimeout(0);

        requester.publish(TestUtils.createSimpleRequestPayload());

        verify(collectorMock, never()).listenForResponses();
        verify(collectorMock, never()).waitForResponses();
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        RequesterImpl<Payload> requester = initRequesterForResponsesWithTimeout(1);

        //doReturn(mock(CollectorManager.class)).when(collectorMock).findOrCreateCollectorManager(anyString());

        requester.publish(TestUtils.createSimpleRequestPayload());

        verify(collectorMock).listenForResponses();
        verify(collectorMock).waitForResponses();
    }

    @Test
    public void testProducerPublishWithPayload() throws Exception {
        String bodyText = "Body text";
        RequesterImpl<Payload> requester = initRequesterForResponsesWithTimeout(0);
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        Payload payload = TestUtils.createPayloadWithTextBody(bodyText);

        requester.publish(payload);

        verify(producerMock).publish(messageCaptor.capture());
        TestUtils.assertRawPayloadContainsBodyText(bodyText, messageCaptor.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAcknowledgeEventHandlerIsAdded() throws Exception {
        Callback onAckMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWithTimeout(1);

        requester.onAcknowledge(onAckMock);

        assertThat(requester.eventHandlers.onAcknowledge(), is(onAckMock));
        assertThat(requester.eventHandlers.onResponse(), not(onAckMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onAckMock));
        assertThat(requester.eventHandlers.onEnd(), not(onAckMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResponseEventHandlerIsAdded() throws Exception {
        Callback onResponseMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWithTimeout(1);

        requester.onResponse(onResponseMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onResponseMock));
        assertThat(requester.eventHandlers.onResponse(), is(onResponseMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onResponseMock));
        assertThat(requester.eventHandlers.onEnd(), not(onResponseMock));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testRawResponseEventHandlerIsAdded() throws Exception {
        Callback onRawResponseMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWithTimeout(1);

        requester.onRawResponse(onRawResponseMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onRawResponseMock));
        assertThat(requester.eventHandlers.onRawResponse(), is(onRawResponseMock));
        assertThat(requester.eventHandlers.onResponse(), not(onRawResponseMock));
        assertThat(requester.eventHandlers.onEnd(), not(onRawResponseMock));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testEndEventHandlerIsAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWithTimeout(1);

        requester.onEnd(onEndMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), is(onEndMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNoEventHandlerAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWithTimeout(1);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), not(onEndMock));
    }

    @Test
    public void testRequestMessage() throws Exception {
        ChannelManager channelManagerMock = mock(ChannelManager.class);
        Producer producerMock = mock(Producer.class);
        when(channelManagerMock.findOrCreateProducer(NAMESPACE)).thenReturn(producerMock);
        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .build();

        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<Payload> requester = RequesterImpl.create(NAMESPACE, TestUtils.createSimpleRequestOptions(), msbContext, new TypeReference<Payload>() {});
        requester.publish(requestPayload);
        verify(producerMock).publish(messageArgumentCaptor.capture());

        Message requestMessage = messageArgumentCaptor.getValue();
        assertNotNull(requestMessage);
        assertNotNull(requestMessage.getMeta());
        assertNotNull(requestMessage.getRawPayload());
    }

    @Test
    public void testRequestMessageWithTags() throws Exception {
        ChannelManager channelManagerMock = mock(ChannelManager.class);
        Producer producerMock = mock(Producer.class);
        when(channelManagerMock.findOrCreateProducer(NAMESPACE)).thenReturn(producerMock);
        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .build();

        String tag = "requester-tag";
        String dynamicTag = "dynamic-tag";
        Payload requestPayload = TestUtils.createSimpleRequestPayload();
        RequestOptions requestOptions = TestUtils.createSimpleRequestOptionsWithTags(tag);

        Requester<Payload> requester = RequesterImpl.create(NAMESPACE, requestOptions, msbContext, new TypeReference<Payload>() {});
        requester.publish(requestPayload, dynamicTag);
        verify(producerMock).publish(messageArgumentCaptor.capture());

        Message requestMessage = messageArgumentCaptor.getValue();
        assertArrayEquals(new String[]{tag, dynamicTag}, requestMessage.getTags().toArray());
    }

    private RequesterImpl<Payload> initRequesterForResponsesWithTimeout(int numberOfResponses) throws Exception {

        MessageTemplate messageTemplateMock = mock(MessageTemplate.class);

        RequestOptions requestOptionsMock = new RequestOptions.Builder().withMessageTemplate(messageTemplateMock).withWaitForResponses(numberOfResponses)
                .withResponseTimeout(100).build();

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .build();

        RequesterImpl<Payload> requester = spy(RequesterImpl.create(NAMESPACE, requestOptionsMock, msbContext, new TypeReference<Payload>() {}));

        collectorMock = spy(new Collector<>(NAMESPACE, TestUtils.createMsbRequestMessageNoPayload(NAMESPACE), requestOptionsMock, msbContext, eventHandlerMock,
                new TypeReference<Payload>() {}));

        doReturn(collectorMock)
                .when(requester)
                .createCollector(anyString(), any(Message.class), any(RequestOptions.class), any(MsbContextImpl.class), any());

        return requester;
    }

}
