package io.github.tcdl.msb.impl;

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
    private Class<? extends  Payload> payloadClass;

    private ResponderServerImpl(String namespace, MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler<? extends Payload> requestHandler, Class<? extends  Payload> payloadClass) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        this.payloadClass = payloadClass;
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
    static ResponderServerImpl create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler) {
        return new ResponderServerImpl(namespace, messageTemplate, msbContext, requestHandler, null);
    }

    /**
     * Create a new instance of {@link ResponderServerImpl}.
     *
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param msbContext      context inside which {@link ResponderServerImpl} is working
     * @param requestHandler  handler for processing the request
     * @param payloadClass  define custom payload object type
     * @return new instance of a {@link ResponderServerImpl}
     */
    static ResponderServerImpl create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler, Class<? extends Payload> payloadClass) {
        return new ResponderServerImpl(namespace, messageTemplate, msbContext, requestHandler, payloadClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.subscribe(namespace,
                message -> {
                        LOG.debug("[{}] Received message with id: [{}]", namespace,message.getId());
                        ResponderImpl responder = new ResponderImpl(messageTemplate, message, msbContext);
                        onResponder(responder);

                }, payloadClass);

        return this;
    }

    void onResponder(ResponderImpl responder) {
        Message originalMessage = responder.getOriginalMessage();
        Payload request = originalMessage.getPayload();
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