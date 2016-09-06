package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Consumer;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Acknowledge;
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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static io.github.tcdl.msb.support.TestUtils.createPayloadWithTextBody;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
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

    private static final String BROADCAST_NAMESPACE = "test:hello:all";
    private static final String MULTICAST_NAMESPACE = "test:hello:everyone";

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
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(0, 0, 0, null, null, null, null);

        publishByAllMethods(requester);

        verify(collectorMock, never()).listenForResponses();
        verify(collectorMock, never()).waitForResponses();
    }

    @Test
    public void testPublishWithRoutingKeyNoWaitForResponses() throws Exception {
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith("UK", 0, 0, 0, null, null, null, null);

        publishByAllMethods(requester);

        verify(collectorMock, never()).listenForResponses();
        verify(collectorMock, never()).waitForResponses();
    }

    @Test
    public void testPublishWaitForResponses() throws Exception {
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

        publishByAllMethods(requester);

        verify(collectorMock, times(4)).listenForResponses();
        verify(collectorMock, times(4)).waitForResponses();
    }

    @Test
    public void testPublishWithRoutingKeyWaitForResponses() throws Exception {
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith("UK", 1, 0, 0, null, null, null, null);

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
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 1000, 800, null, null, null, arg -> fail());

        requester.publish(TestUtils.createSimpleRequestPayload());

        Message responseMessage = TestUtils.createMsbRequestMessage("some:topic", "body text");
        collectorMock.handleMessage(responseMessage, null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPublishHandleErrorResponse() throws Exception {
        RuntimeException ex = new RuntimeException();
        BiConsumer errorHandlerMock = mock(BiConsumer.class);
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(1, 1000, 800, (p, c) -> { throw ex; }, null, errorHandlerMock, arg ->  fail());
        requester.publish(TestUtils.createSimpleRequestPayload());

        Message responseMessage = TestUtils.createMsbRequestMessage("some:topic", "body text");
        collectorMock.handleMessage(responseMessage, null);
        verify(errorHandlerMock).accept(eq(ex), eq(responseMessage));
    }

    @Test
    public void testProducerPublishWithPayload() throws Exception {
        String bodyText = "Body text";
        RequesterImpl<RestPayload> requester = initRequesterForResponsesWith(0, 0, 0, null, null, null, null);
        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        RestPayload payload = createPayloadWithTextBody(bodyText);

        requester.publish(payload);

        verify(producerMock).publish(messageCaptor.capture());
        TestUtils.assertRawPayloadContainsBodyText(bodyText, messageCaptor.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAcknowledgeEventHandlerIsAdded() throws Exception {
        BiConsumer onAckMock = mock(BiConsumer.class);
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

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
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

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
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

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
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

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
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

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
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);

        assertThat(requester.eventHandlers.onAcknowledge(), not(onEndMock));
        assertThat(requester.eventHandlers.onResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onRawResponse(), not(onEndMock));
        assertThat(requester.eventHandlers.onEnd(), not(onEndMock));
        assertThat(requester.eventHandlers.onError(), not(onEndMock));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRequest_customHandlersAreDiscarded() throws Exception {

        BiConsumer<RestPayload, MessageContext> customOnResponseHandler = mock(BiConsumer.class);
        BiConsumer<Message, MessageContext> customOnRawResponseHandler = mock(BiConsumer.class);
        BiConsumer<Acknowledge, MessageContext> customOnAcknowledgeHandler = mock(BiConsumer.class);
        BiConsumer<Exception, Message> customOnErrorHandler = mock(BiConsumer.class);
        Callback<Void> customOnEndHandler = mock(Callback.class);

        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, customOnResponseHandler, customOnAcknowledgeHandler, customOnErrorHandler, customOnEndHandler);

        requester.onRawResponse(customOnRawResponseHandler);

        requester.request(TestUtils.createSimpleRequestPayload());

        assertThat(requester.eventHandlers.onAcknowledge(), not(customOnAcknowledgeHandler));
        assertThat(requester.eventHandlers.onResponse(), not(customOnResponseHandler));
        assertThat(requester.eventHandlers.onRawResponse(), not(customOnRawResponseHandler));
        assertThat(requester.eventHandlers.onEnd(), not(customOnEndHandler));
        assertThat(requester.eventHandlers.onError(), not(customOnErrorHandler));
    }

    @Test
    public void testRequest_responseHandlerCompletesFuture() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        assertFalse(futureResult.isDone());

        RestPayload mockResponsePayload = mock(RestPayload.class);
        MessageContext mockMessageContext = mock(MessageContext.class);

        requester.eventHandlers.onResponse().accept(mockResponsePayload, mockMessageContext);
        assertTrue(futureResult.isDone());
        assertEquals(mockResponsePayload, futureResult.get());
    }

    @Test
    public void testRequest_rawResponseHandlerDoesNotCompleteFuture() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        assertFalse(futureResult.isDone());

        Message responseMessage = TestUtils.createSimpleResponseMessage("anyNamespace");
        MessageContext mockMessageContext = mock(MessageContext.class);

        requester.eventHandlers.onRawResponse().accept(responseMessage, mockMessageContext);
        assertFalse(futureResult.isDone());
    }

    @Test
    public void testRequest_errorHandlerCancelsFuture() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        assertFalse(futureResult.isDone());

        Message responseMessage = TestUtils.createSimpleResponseMessage("anyNamespace");
        Exception e = new Exception("some message");

        requester.eventHandlers.onError().accept(e, responseMessage);
        assertTrue(futureResult.isCancelled());
    }

    @Test
    public void testRequest_endHandlerCancelsNotCompletedFuture() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        assertFalse(futureResult.isDone());

        requester.eventHandlers.onEnd().call(null);
        assertTrue(futureResult.isCancelled());
    }

    @Test
    public void testRequest_endHandlerDoesNothingWithCompletedFuture() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        RestPayload mockResponsePayload = mock(RestPayload.class);
        MessageContext mockMessageContext = mock(MessageContext.class);

        requester.eventHandlers.onResponse().accept(mockResponsePayload, mockMessageContext);
        requester.eventHandlers.onEnd().call(null);
        assertFalse(futureResult.isCancelled());
    }

    @Test
    public void testRequest_acknowledgeHandlerCancelsFutureOnNoResponses() throws Exception {
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        MessageContext mockMessageContext = mock(MessageContext.class);
        Acknowledge acknowledge = new Acknowledge.Builder()
                .withResponderId("responderId")
                .withResponsesRemaining(0)
                .withTimeoutMs(0)
                .build();

        requester.eventHandlers.onAcknowledge().accept(acknowledge, mockMessageContext);
        assertTrue(futureResult.isCancelled());
    }

    @Test
    public void testRequest_acknowledgeHandlerCancelsFutureOnTooManyResponses() throws Exception{
        RequesterImpl requester = initRequesterForResponsesWith(1, 0, 0, null, null, null, null);
        CompletableFuture futureResult = requester.request(TestUtils.createSimpleRequestPayload());

        MessageContext mockMessageContext = mock(MessageContext.class);
        Acknowledge acknowledge = new Acknowledge.Builder()
                .withResponderId("responderId")
                .withResponsesRemaining(2)
                .withTimeoutMs(0)
                .build();

        requester.eventHandlers.onAcknowledge().accept(acknowledge, mockMessageContext);
        assertTrue(futureResult.isCancelled());
    }

    @Test
    public void testRequestMessage() throws Exception {
        ChannelManager channelManagerMock = mock(ChannelManager.class);
        Producer producerMock = mock(Producer.class);
        when(channelManagerMock.findOrCreateProducer(BROADCAST_NAMESPACE)).thenReturn(producerMock);
        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .build();

        RestPayload requestPayload = TestUtils.createSimpleRequestPayload();
        Requester<RestPayload> requester = RequesterImpl.create(BROADCAST_NAMESPACE, TestUtils.createSimpleRequestOptions(), msbContext, new TypeReference<RestPayload>(){});
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
        when(channelManagerMock.findOrCreateProducer(BROADCAST_NAMESPACE)).thenReturn(producerMock);
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

        Requester<RestPayload> requester = RequesterImpl.create(BROADCAST_NAMESPACE, requestOptions, msbContext, new TypeReference<RestPayload>(){});
        requester.publish(requestPayload, dynamicTag1, dynamicTag2, nullTag);
        verify(producerMock).publish(messageArgumentCaptor.capture());

        Message requestMessage = messageArgumentCaptor.getValue();
        assertArrayEquals(new String[]{tag, dynamicTag1, dynamicTag2}, requestMessage.getTags().toArray());
    }

    @Test
    public void testRequestMessageWithForward_shouldNotWaitForResponsesOrAcks() throws Exception {
        String routingKey = "to.santa";

        ChannelManager channelManagerMock = mock(ChannelManager.class);
        Producer producerMock = mock(Producer.class);
        when(channelManagerMock.findOrCreateProducer(BROADCAST_NAMESPACE)).thenReturn(producerMock);
        ArgumentCaptor<Message> messageArgumentCaptor = ArgumentCaptor.forClass(Message.class);

        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .withClock(Clock.systemDefaultZone())
                .build();

        RestPayload requestPayload = TestUtils.createSimpleRequestPayload();
        RequestOptions requestOptions = new RequestOptions
                .Builder()
                .withRoutingKey(routingKey)
                .withWaitForResponses(3)
                .withAckTimeout(1)
                .withForwardNamespace(MULTICAST_NAMESPACE).build();

        RequesterImpl<RestPayload> requesterSpy = spy(RequesterImpl.create(BROADCAST_NAMESPACE, requestOptions, msbContext, new TypeReference<RestPayload>() {}));
        requesterSpy.publish(requestPayload);

        verify(producerMock).publish(messageArgumentCaptor.capture());

        //check collector wasn't set up
        verify(requesterSpy, never()).createCollector(any(), any(), any(), any(), anyBoolean());
        verify(channelManagerMock, never()).findOrCreateProducer(any(MessageDestination.class));

        //check message fields
        Message requestMessage = messageArgumentCaptor.getValue();
        assertEquals(MULTICAST_NAMESPACE, requestMessage.getTopics().getForward());
        assertEquals(routingKey, requestMessage.getTopics().getRoutingKey());
    }

    private RequesterImpl<RestPayload> initRequesterForResponsesWith(Integer numberOfResponses, Integer respTimeout, Integer ackTimeout,
                                                                     BiConsumer<RestPayload, MessageContext> onResponse, BiConsumer<Acknowledge, MessageContext> onAcknowledge,
                                                                     BiConsumer<Exception, Message> onError,
                                                                     Callback<Void> endHandler) throws Exception {

        RequestOptions requestOptions = new RequestOptions.Builder()
                .withMessageTemplate(mock(MessageTemplate.class))
                .withWaitForResponses(numberOfResponses)
                .withResponseTimeout(respTimeout)
                .withAckTimeout(ackTimeout)
                .build();

        when(channelManagerMock.findOrCreateProducer(anyString())).thenReturn(producerMock);

        return setUpRequester(BROADCAST_NAMESPACE, onResponse, onAcknowledge, onError, endHandler, requestOptions);
    }

    private RequesterImpl<RestPayload> initRequesterForResponsesWith(String routingKey, Integer numberOfResponses, Integer respTimeout, Integer ackTimeout,
                                                                     BiConsumer<RestPayload, MessageContext> onResponse, BiConsumer<Acknowledge, MessageContext> onAcknowledge,
                                                                     BiConsumer<Exception, Message> onError,
                                                                     Callback<Void> endHandler) throws Exception {

        RequestOptions requestOptions = new RequestOptions.Builder()
                .withMessageTemplate(mock(MessageTemplate.class))
                .withWaitForResponses(numberOfResponses)
                .withResponseTimeout(respTimeout)
                .withRoutingKey(routingKey)
                .withAckTimeout(ackTimeout)
                .build();

        when(channelManagerMock.findOrCreateProducer(any(MessageDestination.class))).thenReturn(producerMock);

        return setUpRequester(MULTICAST_NAMESPACE, onResponse, onAcknowledge, onError, endHandler, requestOptions);
    }

    private RequesterImpl<RestPayload> setUpRequester(String namespace, BiConsumer<RestPayload, MessageContext> onResponse, BiConsumer<Acknowledge, MessageContext> onAcknowledge, BiConsumer<Exception, Message> onError, Callback<Void> endHandler, RequestOptions requestOptions) {
        MsbContextImpl msbContext = TestUtils.createMsbContextBuilder()
                .withChannelManager(channelManagerMock)
                .build();

        RequesterImpl<RestPayload> requester = spy(RequesterImpl.create(namespace, requestOptions, msbContext, new TypeReference<RestPayload>() {
        }));
        requester.onResponse(onResponse)
                .onError(onError)
                .onAcknowledge(onAcknowledge)
                .onEnd(endHandler);

        collectorMock = spy(new Collector<>(namespace, TestUtils.createMsbRequestMessageNoPayload(namespace), requestOptions, msbContext, requester.eventHandlers,
                new TypeReference<RestPayload>() {
                }));

        doReturn(collectorMock)
                .when(requester)
                .createCollector(any(Message.class), any(RequestOptions.class), any(MsbContextImpl.class), any(), anyBoolean());
        return requester;
    }
}
