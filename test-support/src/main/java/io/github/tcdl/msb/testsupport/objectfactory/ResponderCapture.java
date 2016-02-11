package io.github.tcdl.msb.testsupport.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.introspect.ClassIntrospector;
import io.github.tcdl.msb.api.Callback;
import io.github.tcdl.msb.api.MessageContext;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import org.mockito.ArgumentCaptor;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Captured params of a responder, including a requestHandler.
 * @param <T>
 */
public class ResponderCapture<T> extends AbstractCapture <T> {
    private final MessageTemplate messageTemplate;
    private final ResponderServer.RequestHandler<T> requestHandler;
    private final ResponderServer responderServerMock;

    public ResponderCapture(String namespace, MessageTemplate messageTemplate, ResponderServer.RequestHandler<T> requestHandler,
            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        super(namespace, payloadTypeReference, payloadClass);
        this.messageTemplate = messageTemplate;
        this.requestHandler = requestHandler;
        this.responderServerMock = mock(ResponderServer.class);
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    public ResponderServer.RequestHandler<T> getRequestHandler() {
        return requestHandler;
    }

    public ResponderServer getResponderServerMock() {
        return responderServerMock;
    }
}