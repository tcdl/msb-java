package io.github.tcdl.msb.mock.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ResponderServer;

import static org.mockito.Mockito.mock;

/**
 * Captured params of a responder, including a requestHandler.
 * @param <T>
 */
public class ResponderCapture<T> extends AbstractCapture <T> {
    private final MessageTemplate messageTemplate;
    private final ResponderServer.RequestHandler<T> requestHandler;
    private final ResponderServer.ErrorHandler errorHandler;
    private final ResponderServer responderServerMock;

    public ResponderCapture(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler,
            ResponderServer.ErrorHandler errorHandler,
            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        super(namespace, payloadTypeReference, payloadClass);
        this.messageTemplate = messageTemplate;
        this.requestHandler = requestHandler;
        this.errorHandler = errorHandler;
        this.responderServerMock = mock(ResponderServer.class);
    }

    public MessageTemplate getMessageTemplate() {
        return messageTemplate;
    }

    public ResponderServer.RequestHandler<T> getRequestHandler() {
        return requestHandler;
    }

    public ResponderServer.ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public ResponderServer getResponderServerMock() {
        return responderServerMock;
    }
}