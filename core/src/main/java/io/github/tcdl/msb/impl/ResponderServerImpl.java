package io.github.tcdl.msb.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.ResponderServer;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.payload.Payload;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderServerImpl implements ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServerImpl.class);

    private String namespace;
    private MsbContextImpl msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler requestHandler;
    private ObjectMapper payloadMapper;
    private Class<? extends Payload> payloadClass;

    private ResponderServerImpl(String namespace, MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler, Class<? extends Payload> payloadClass) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.payloadMapper = msbContext.getPayloadMapper();
        this.payloadClass = payloadClass;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * {@link io.github.tcdl.msb.api.ObjectFactory#createResponderServer(String, MessageTemplate, RequestHandler, Class)}
     */
    static <T extends Payload> ResponderServerImpl create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext,
            RequestHandler<T> requestHandler, Class<T> payloadClass) {
        return new ResponderServerImpl(namespace, messageTemplate, msbContext, requestHandler, payloadClass);
    }

    /**
     * {@inheritDoc}
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
        Payload request = payloadMapper.convertValue(rawPayload, payloadClass);
        LOG.debug("[{}] Process message with id: [{}]", namespace, originalMessage.getId());
        try {
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