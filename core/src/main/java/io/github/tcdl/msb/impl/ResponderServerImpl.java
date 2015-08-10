package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderServerImpl<T extends Payload> implements ResponderServer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServerImpl.class);

    private String namespace;
    private MsbContextImpl msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler<T> requestHandler;
    private ObjectMapper payloadMapper;
    private TypeReference<T> payloadTypeReference;

    private ResponderServerImpl(String namespace, MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.payloadMapper = msbContext.getPayloadMapper();
        this.payloadTypeReference = payloadTypeReference;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * {@link io.github.tcdl.msb.api.ObjectFactory#createResponderServer(String, MessageTemplate, RequestHandler, Class)}
     */
    static <T extends Payload> ResponderServerImpl<T> create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext,
            RequestHandler<T> requestHandler, TypeReference<T> payloadTypeReference) {
        return new ResponderServerImpl<>(namespace, messageTemplate, msbContext, requestHandler, payloadTypeReference);
    }

    /**
     * Start listening for message on specified topic.
     *
     * In case of exception was thrown during message conversion or handling business logic
     * response message with statusCode=500 and statusMessage=${exception.getMessage()} will be created and sent automatically.
     */
    @Override
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.subscribe(namespace,
                incomingMessage -> {
                    LOG.debug("[{}] Received message with id: [{}]", namespace, incomingMessage.getId());
                    ResponderImpl responder = new ResponderImpl(messageTemplate, incomingMessage, msbContext);
                    onResponder(responder);
                });

        return this;
    }

    void onResponder(ResponderImpl responder) {
        Message originalMessage = responder.getOriginalMessage();
        Object rawPayload = originalMessage.getRawPayload();
        try {
            T request = Utils.convert(rawPayload, payloadTypeReference, payloadMapper);
            LOG.debug("[{}] Process message with id: [{}]", namespace, originalMessage.getId());
            requestHandler.process(request, responder);
        } catch (Exception exception) {
            errorHandler(responder, exception);
        }
    }

    private void errorHandler(Responder responder, Exception exception) {
        Message originalMessage = responder.getOriginalMessage();
        LOG.error("[{}] Error while processing message with id: [{}]. Cause: [{}]", namespace, originalMessage.getId(), exception.getMessage());
        Payload responsePayload = new Payload.Builder()
                .withStatusCode(500)
                .withStatusMessage(exception.getMessage()).build();
        responder.send(responsePayload);
    }
}