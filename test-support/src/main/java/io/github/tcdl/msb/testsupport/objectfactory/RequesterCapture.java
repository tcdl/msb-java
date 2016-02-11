package io.github.tcdl.msb.testsupport.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.RequestOptions;
import io.github.tcdl.msb.api.Requester;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import org.mockito.ArgumentCaptor;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Captured params of a requester. Handlers are captured by {@link ArgumentCaptor}.
 * @param <T>
 */
public class RequesterCapture<T> extends AbstractCapture <T> {
    private final RequestOptions requestOptions;
    private final Requester<T> requesterMock;

    private final ArgumentCaptor<BiConsumer<Acknowledge, MessageContext>> onAcknowledgeCaptor
            = ArgumentCaptor.forClass((Class<BiConsumer<Acknowledge, MessageContext>>)(Class)BiConsumer.class);

    private final ArgumentCaptor<BiConsumer<T, MessageContext>> onResponseCaptor
            = ArgumentCaptor.forClass((Class<BiConsumer<T, MessageContext>>)(Class)BiConsumer.class);

    private final ArgumentCaptor<BiConsumer<Message, MessageContext>> onRawResponseCaptor
            = ArgumentCaptor.forClass((Class<BiConsumer<Message, MessageContext>>)(Class)BiConsumer.class);

    private final ArgumentCaptor<Callback<Void>> onEndCaptor
            = ArgumentCaptor.forClass((Class<Callback<Void>>)(Class)Callback.class);

    public RequesterCapture(String namespace, RequestOptions requestOptions,
            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        super(namespace, payloadTypeReference, payloadClass);
        this.requestOptions = requestOptions;
        this.requesterMock = mock(Requester.class);

        when(this.requesterMock.onAcknowledge(onAcknowledgeCaptor.capture())).thenReturn(this.requesterMock);
        when(this.requesterMock.onResponse(onResponseCaptor.capture())).thenReturn(this.requesterMock);
        when(this.requesterMock.onRawResponse(onRawResponseCaptor.capture())).thenReturn(this.requesterMock);
        when(this.requesterMock.onEnd(onEndCaptor.capture())).thenReturn(this.requesterMock);

    }

    public RequestOptions getRequestOptions() {
        return requestOptions;
    }

    public Requester<T> getRequesterMock() {
        return requesterMock;
    }

    public ArgumentCaptor<BiConsumer<Acknowledge, MessageContext>> getOnAcknowledgeCaptor() {
        return onAcknowledgeCaptor;
    }

    public ArgumentCaptor<BiConsumer<T, MessageContext>> getOnResponseCaptor() {
        return onResponseCaptor;
    }

    public ArgumentCaptor<BiConsumer<Message, MessageContext>> getOnRawResponseCaptor() {
        return onRawResponseCaptor;
    }

    public ArgumentCaptor<Callback<Void>> getOnEndCaptor() {
        return onEndCaptor;
    }
}