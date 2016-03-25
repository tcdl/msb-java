package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Consumer;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.RestPayload;
import io.github.tcdl.msb.collector.Collector;
import io.github.tcdl.msb.support.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Clock;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by rdro on 4/27/2015.
 */
@RunWith(MockitoJUnitRunner.class)
public class RequesterImplTest {

    private static final String NAMESPACE = "test:requester";

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
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(0, 0, 0, null, null, null);

        publishByAllMethods(requester);

        verify(collectorMock, never()).listenForResponses();
        verify(collectorMock, never()).waitForResponses();
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 0, 0, null, null, null);

        publishByAllMethods(requester);

        verify(collectorMock, times(4)).listenForResponses();
        verify(collectorMock, times(4)).waitForResponses();
    }

    private void publishByAllMethods(RequesterImpl<RestPayload> requester) {
        Message originalMessage = TestUtils.createMsbRequestMessage("some:topic", "body text");
        requester.publish(TestUtils.createSimpleRequestPayload());
        requester.publish(TestUtils.createSimpleRequestPayload(), "tag", "anotherTag");
        requester.publish(TestUtils.createSimpleRequestPayload(), originalMessage);
        requester.publish(TestUtils.createSimpleRequestPayload(), originalMessage, "tag", "anotherTag");
    }

    @Test
    public void testPublishWaitForResponsesAck() throws Exception {
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 1000, 800, null, null, arg ->  fail());

        requester.publish(TestUtils.createSimpleRequestPayload());

        Message responseMessage = TestUtils.createMsbRequestMessage("some:topic", "body text");
        collectorMock.handleMessage(responseMessage, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishHandleErrorResponse() throws Exception {
        RuntimeException ex = new RuntimeException();
        BiConsumer errorHandlerMock = mock(BiConsumer.class);
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 1000, 800, (p, c) -> { throw ex; }, errorHandlerMock, arg ->  fail());
        requester.publish(TestUtils.createSimpleRequestPayload());

        Message responseMessage = TestUtils.createMsbRequestMessage("some:topic", "body text");
        collectorMock.handleMessage(responseMessage, null);
        verify(errorHandlerMock).accept(eq(ex), eq(responseMessage));
    }

    @Test
    public void testProducerPublishWithPayload() throws Exception {
        String bodyText = "Body text";
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(0, 0, 0, null, null, null);
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        RestPayload payload = TestUtils.createPayloadWithTextBody(bodyText);

        requester.publish(payload);

        verify(producerMock).publish(messageCaptor.capture());
        TestUtils.assertRawPayloadContainsBodyText(bodyText, messageCaptor.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAcknowledgeEventHandlerIsAdded() throws Exception {
        BiConsumer onAckMock = mock(BiConsumer.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null);

        requester.onAcknowledge(onAckMock);

        assertThat(requester.eventHandlers.onAcknowledge(), is(onAckMock));
        assertThat(requester.eventHandlers.onResponse(), not(onAckMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onAckMock));
        assertThat(requester.eventHandlers.onEnd(), not(onAckMock));
        assertThat(requester.eventHandlers.onError(), not(onAckMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testResponseEventHandlerIsAdded() throws Exception {
        BiConsumer onResponseMock = mock(BiConsumer.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null);

        requester.onResponse(onResponseMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onResponseMock));
        assertThat(requester.eventHandlers.onResponse(), is(onResponseMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onResponseMock));
        assertThat(requester.eventHandlers.onEnd(), not(onResponseMock));
        assertThat(requester.eventHandlers.onError(), not(onResponseMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRawResponseEventHandlerIsAdded() throws Exception {
        BiConsumer onRawResponseMock = mock(BiConsumer.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null);

        requester.onRawResponse(onRawResponseMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onRawResponseMock));
        assertThat(requester.eventHandlers.onRawResponse(), is(onRawResponseMock));
        assertThat(requester.eventHandlers.onResponse(), not(onRawResponseMock));
        assertThat(requester.eventHandlers.onEnd(), not(onRawResponseMock));
        assertThat(requester.eventHandlers.onError(), not(onRawResponseMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEndEventHandlerIsAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0,null, null, null);

        requester.onEnd(onEndMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), is(onEndMock));
        assertThat(requester.eventHandlers.onError(), not(onEndMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testErrorEventHandlerIsAdded() throws Exception {
        BiConsumer onErrorMock = mock(BiConsumer.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0,null, null, null);

        requester.onError(onErrorMock);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onErrorMock));
        assertThat(requester.eventHandlers.onResponse(), not(onErrorMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onErrorMock));
        assertThat(requester.eventHandlers.onEnd(), not(onErrorMock));
        assertThat(requester.eventHandlers.onError(), is(onErrorMock));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNoEventHandlerAdded() throws Exception {
        Callback onEndMock = mock(Callback.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), not(onEndMock));
        assertThat(requester.eventHandlers.onError(), not(onEndMock));
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

        RestPayload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<RestPayload> requester = RequesterImpl.create(NAMESPACE, TestUtils.createSimpleRequestOptions(), msbContext, new TypeReference<RestPayload>() {});
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
        String dynamicTag1 = "dynamic-tag1";
        String dynamicTag2 = "dynamic-tag2";
        String nullTag = null;
        RestPayload requestPayload = TestUtils.createSimpleRequestPayload();
        RequestOptions requestOptions = TestUtils.createSimpleRequestOptionsWithTags(tag);

        Requester<RestPayload> requester = RequesterImpl.create(NAMESPACE, requestOptions, msbContext, new TypeReference<RestPayload>() {});
        requester.publish(requestPayload, dynamicTag1, dynamicTag2, nullTag);
        verify(producerMock).publish(messageArgumentCaptor.capture());

        Message requestMessage = messageArgumentCaptor.getValue();
        assertArrayEquals(new String[]{tag, dynamicTag1, dynamicTag2}, requestMessage.getTags().toArray());
    }

    @Test
    public void testRequestMessageWithForward() throws Exception {
        String forwardNamespace = "test:forward";
        ChannelManager channelManagerMock = mock(ChannelManager.class);
        Producer producerMock = mock(Producer.class);
        when(channelManagerMock.findOrCreateProducer(NAMESPACE)).thenReturn(producerMock);
        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .build();

        RestPayload requestPayload = TestUtils.createSimpleRequestPayload();
        RequestOptions requestOptions = new RequestOptions
                .Builder()
                .withForwardNamespace(forwardNamespace).build();

        Requester<RestPayload> requester = RequesterImpl.create(NAMESPACE, requestOptions, msbContext, new TypeReference<RestPayload>() {});
        requester.publish(requestPayload);
        verify(producerMock).publish(messageArgumentCaptor.capture());

        Message requestMessage = messageArgumentCaptor.getValue();
        assertEquals(forwardNamespace, requestMessage.getTopics().getForward());
    }

    private RequesterImpl<RestPayload> initRequesterForResponsesWith(Integer numberOfResponses, Integer respTimeout,  Integer ackTimeout,
            BiConsumer<RestPayload, MessageContext> onResponse,
            BiConsumer<Exception, Message> onError,
            Callback<Void> endHandler) throws Exception {

        MessageTemplate messageTemplateMock = mock(MessageTemplate.class);

        RequestOptions requestOptionsMock = new RequestOptions.Builder()
                .withMessageTemplate(messageTemplateMock)
                .withWaitForResponses(numberOfResponses)
                .withResponseTimeout(respTimeout)
                .withAckTimeout(ackTimeout)
                .build();

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .build();

        RequesterImpl<RestPayload> requester = spy(RequesterImpl.create(NAMESPACE, requestOptionsMock, msbContext, new TypeReference<RestPayload>() {}));
        requester.onResponse(onResponse)
                 .onError(onError)
                 .onEnd(endHandler);

        collectorMock = spy(new Collector<>(NAMESPACE, TestUtils.createMsbRequestMessageNoPayload(NAMESPACE), requestOptionsMock, msbContext, requester.eventHandlers,
                new TypeReference<RestPayload>() {}));

        doReturn(collectorMock)
                .when(requester)
                .createCollector(anyString(), any(Message.class), any(RequestOptions.class), any(MsbContextImpl.class), any());

        return requester;
    }

}
