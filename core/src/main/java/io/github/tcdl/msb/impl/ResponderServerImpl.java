package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.MessageHandler;
import io.github.tcdl.msb.api.*;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

public class ResponderServerImpl<T> implements ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServerImpl.class);

    private String namespace;
    private ResponderOptions responderOptions;
    private MsbContextImpl msbContext;
    private RequestHandler<T> requestHandler;
    private Optional<ErrorHandler> errorHandler;
    private ObjectMapper payloadMapper;
    private TypeReference<T> payloadTypeReference;

    private ResponderServerImpl(String namespace,
                                ResponderOptions responderOptions,
                                MsbContextImpl msbContext,
                                RequestHandler<T> requestHandler,
                                ErrorHandler errorHandler,
                                TypeReference<T> payloadTypeReference) {
        Validate.notNull(responderOptions);
        this.namespace = namespace;
        this.responderOptions = responderOptions;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.errorHandler = Optional.ofNullable(errorHandler);
        this.payloadMapper = msbContext.getPayloadMapper();
        this.payloadTypeReference = payloadTypeReference;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * {@link io.github.tcdl.msb.api.ObjectFactory#createResponderServer(String, MessageTemplate, RequestHandler, ErrorHandler, Class)}
     */
    static <T> ResponderServerImpl<T> create(String namespace, ResponderOptions responderOptions, MsbContextImpl msbContext,
            RequestHandler<T> requestHandler,  ErrorHandler errorHandler, TypeReference<T> payloadTypeReference) {
        return new ResponderServerImpl<>(namespace, responderOptions, msbContext, requestHandler, errorHandler, payloadTypeReference);
    }

    /**
     * Start listening for message on specified topic.
     *
     * If exception was thrown during handling business logic response message with statusCode={@link #INTERNAL_SERVER_ERROR_CODE}
     * and statusMessage=${exception.getMessage()} will be created and sent automatically.
     * If error occurred during message payload conversion to specified type response message with statusCode={@link #PAYLOAD_CONVERSION_ERROR_CODE }
     * and statusMessage="Failed to convert object [${jsonPayload}] to type T}" will be created and sent automatically.
     */
    @Override
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();

        MessageHandler messageHandler = (incomingMessage, acknowledgeHandler) -> {
            LOG.debug("[{}] Received message with id: [{}]", namespace, incomingMessage.getId());
            Responder responder = createResponder(incomingMessage);
            ResponderContext responderContext = createResponderContext(responder, acknowledgeHandler, incomingMessage);
            onResponder(responderContext);
        };

        channelManager.subscribe(namespace, responderOptions, messageHandler);
        return this;
    }

    @Override
    public ResponderServer stop(){
        ChannelManager channelManager = msbContext.getChannelManager();
        channelManager.unsubscribe(namespace);
        return this;
    }

    @Override
    public Optional<Long> availableMessageCount() {
        return msbContext.getChannelManager().getAvailableMessageCount(namespace);
    }

    Responder createResponder(Message incomingMessage) {
        if (isResponseNeeded(incomingMessage)) {
            return new ResponderImpl(responderOptions.getMessageTemplate(), incomingMessage, msbContext);
        } else {
            return new NoopResponderImpl(incomingMessage);
        }
    }

    ResponderContext createResponderContext(Responder responder, AcknowledgementHandler acknowledgeHandler, Message incomingMessage) {
        return new ResponderContextImpl(responder, acknowledgeHandler, incomingMessage);
    }

    void onResponder(ResponderContext responderContext) {
        Message originalMessage = responderContext.getOriginalMessage();
        Object rawPayload = originalMessage.getRawPayload();
        try {
            T request = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
            MsbThreadContext.setMessageContext(responderContext);
            MsbThreadContext.setRequest(request);

            LOG.debug("[{}] Process message with id: [{}]", namespace, originalMessage.getId());
            requestHandler.process(request, responderContext);
        } catch (Exception e) {
            if (errorHandler.isPresent()) {
                errorHandler.get().handle(e, originalMessage);
            } else {
                errorHandler(responderContext, e);
            }
        } finally {
            MsbThreadContext.clear();
        }
    }

    private boolean isResponseNeeded(Message incomingMessage) {
        return incomingMessage.getTopics().getResponse() != null;
    }

    private void errorHandler(ResponderContext responderContext, Exception exception) {
        Message originalMessage = responderContext.getOriginalMessage();
        LOG.error("[{}] Error while processing message with id: [{}]", namespace, originalMessage.getId(), exception);
        responderContext.getResponder().sendAck(0, 0);
        //Confirm message for prevention requeue message with incorrect structure
        responderContext.getAcknowledgementHandler().confirmMessage();
    }
}