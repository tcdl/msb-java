package io.github.tcdl.msb.mock.objectfactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.ResponderServer;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * Captured params of a responder, including a requestHandler.
 * @param <T>
 */
public class ResponderCapture<T> extends AbstractCapture <T> {
    private final MessageTemplate messageTemplate;
    private final Set<String> routingKeys;
    private final ResponderServer.RequestHandler<T> requestHandler;
    private final ResponderServer.ErrorHandler errorHandler;
    private final ResponderServer responderServerMock;

    public ResponderCapture(String namespace, MessageTemplate messageTemplate,
            ResponderServer.RequestHandler<T> requestHandler,
            ResponderServer.ErrorHandler errorHandler,
            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        this(namespace, Collections.emptySet(), messageTemplate, requestHandler, errorHandler, payloadTypeReference, payloadClass);
    }

    public ResponderCapture(String namespace, Set<String> routingKeys, MessageTemplate messageTemplate,
                            ResponderServer.RequestHandler<T> requestHandler,
                            ResponderServer.ErrorHandler errorHandler,
                            TypeReference<T> payloadTypeReference, Class<T> payloadClass) {
        super(namespace, payloadTypeReference, payloadClass);
        this.routingKeys = routingKeys;
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

    public Set<String> getRoutingKeys() {
        return Collections.unmodifiableSet(routingKeys);
    }
}