package io.github.tcdl.msb.impl;

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

public class ResponderServerImpl implements ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServerImpl.class);

    private String namespace;
    private MsbContextImpl msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler requestHandler;
    private Class payloadClass;

    private ResponderServerImpl(String namespace, MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler, Class payloadClass) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.payloadClass = Utils.ifNull(payloadClass, Payload.class);
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * Create a new instance of {@link ResponderServerImpl}.
     *
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param msbContext      context inside which {@link ResponderServerImpl} is working
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServerImpl}
     */
    static ResponderServerImpl create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler, Class payloadClass) {
        return new ResponderServerImpl(namespace, messageTemplate, msbContext, requestHandler, payloadClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();
        ObjectMapper messageMapper = msbContext.getMessageMapper();

        channelManager.subscribe(namespace,
                incomingMessage -> {
                        LOG.debug("Received message with id {} from topic {}", incomingMessage.getId(), namespace);
                        Message message = Utils.fromJson(Utils.toJson(incomingMessage, messageMapper), Message.class, payloadClass, messageMapper);
                        ResponderImpl responder = new ResponderImpl(messageTemplate, message, msbContext);
                        onResponder(responder);
                });

        return this;
    }

    void onResponder(ResponderImpl responder) {
        Message originalMessage = responder.getOriginalMessage();
        Payload request = originalMessage.getPayload();
        LOG.debug("Pushing message with id {} to middleware chain", originalMessage.getId());
        try {
            requestHandler.process(request, responder);
        } catch (Exception exception) {
            errorHandler(responder, exception);
        }
    }

    private void errorHandler(Responder responder, Exception exception) {
        Message originalMessage = responder.getOriginalMessage();
        LOG.error("Handling error for message with id {}", originalMessage.getId());
        Payload responsePayload = new Payload.Builder()
                .withStatusCode(500)
                .withStatusMessage(exception.getMessage()).build();
        responder.send(responsePayload);
    }
}