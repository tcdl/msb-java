package io.github.tcdl.api;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.MsbContextImpl;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.impl.ResponderImpl;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for creating microservices responsible for listening on messages from the bus and executing business logic. *
 * <p>First of all it's needed to create {@code ResponderServer} with
 * {@link #create(String, MessageTemplate, MsbContextImpl, RequestHandler)} method.
 * Then we can start listening incoming messages, by calling it's {@link #listen()} method.
 * And also it's required to implement interface {@link RequestHandler}. Implementation of this interface will be
 * business logic processed by microservice. Inside this logic we can use instance of {@link Responder} created by {@code ResponderServer}
 * for each message from bus, and can be used for sending responses back to bus.
 */
public class ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServer.class);

    private String namespace;
    private MsbContextImpl msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler requestHandler;

    private ResponderServer(String namespace, MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * Create a new instance of {@link ResponderServer}.
     *
     * @param namespace       topic on a bus for listening on incoming requests
     * @param messageTemplate template used for creating response messages
     * @param msbContext      context inside which {@link ResponderServer} is working
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServer}
     */
    public static ResponderServer create(String namespace,  MessageTemplate messageTemplate, MsbContextImpl msbContext, RequestHandler requestHandler) {
        return new ResponderServer(namespace, messageTemplate, msbContext, requestHandler);
    }

    /**
     * Start listening for message on specified topic.
     */
    public ResponderServer listen() {
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.subscribe(namespace,
                message -> {
                        LOG.debug("Received message with id {} from topic {}", message.getId(), namespace);
                        ResponderImpl responder = new ResponderImpl(messageTemplate, message, msbContext);
                        onResponder(responder);

                });

        return this;
    }

    /**
     * Implementation of this interface contains business logic processed by microservice.
     */
    public interface RequestHandler {
        /**
         * Execute business logic and send response.
         * @param request request received from a bus
         * @param responder object of type {@link Responder} which will be used for sending response
         * @throws Exception if some problems during execution business logic or sending response were occurred
         */
        void process(Payload request, Responder responder) throws Exception;
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

    private void errorHandler(ResponderImpl responder, Exception exception) {
        Message originalMessage = responder.getOriginalMessage();
        LOG.error("Handling error for message with id {}", originalMessage.getId());
        Payload responsePayload = new Payload.PayloadBuilder()
                .withStatusCode(500)
                .withStatusMessage(exception.getMessage()).build();
        responder.send(responsePayload);
    }
}