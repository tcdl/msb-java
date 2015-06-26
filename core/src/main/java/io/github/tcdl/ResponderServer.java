package io.github.tcdl;

import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for creating microservices which will listen for incoming requests, execute business logic
 * and respond.
 * <p>First of all it's needed to create {@code ResponderServer}, method create
 * {@link #create(String, MessageTemplate, MsbContext, RequestHandler)} is responsible for it.
 * After object of {@code ResponderServer} was created it should start listening incoming messages.
 * Method {@link #listen} is used to listen incoming messages.
 * And also it's needed to implement interface {@link RequestHandler}.Implementation of this interface will be
 * business logic of microservice.Inside this login {@link Responder} can be used for sending response.
 */
public class ResponderServer {
    private static final Logger LOG = LoggerFactory.getLogger(ResponderServer.class);

    private String namespace;
    private MsbContext msbContext;
    private MessageTemplate messageTemplate;
    private RequestHandler requestHandler;

    private ResponderServer(String namespace, MessageTemplate messageTemplate, MsbContext msbContext, RequestHandler requestHandler) {
        this.namespace = namespace;
        this.messageTemplate = messageTemplate;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * Create a new instance of a {@link ResponderServer}.
     *
     * @param namespace       topic on a bus for listening incoming requests
     * @param messageTemplate template which will be used for creating response messages, object of class {@link MessageTemplate}
     * @param msbContext      context inside which {@link ResponderServer} is working
     * @param requestHandler  handler for processing the request
     * @return new instance of a {@link ResponderServer}
     */
    public static ResponderServer create(String namespace,  MessageTemplate messageTemplate, MsbContext msbContext, RequestHandler requestHandler) {
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
                        Responder responder = new Responder(messageTemplate, message, msbContext);
                        onResponder(responder);

                });

        return this;
    }

    /**
     * Implementation of this interface is business logic of microservice.
     */
    public interface RequestHandler {
        /**
         * Execute business login and send response.
         * @param request request which was received from a bus
         * @param responder object of class {@link Responder} which will be used for sending response
         * @throws Exception if some problems during execution business logic or sending response were occurred
         */
        void process(Payload request, Responder responder) throws Exception;
    }

    protected void onResponder(Responder responder) {
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
        Payload responsePayload = new Payload.PayloadBuilder()
                .withStatusCode(500)
                .withStatusMessage(exception.getMessage()).build();
        responder.send(responsePayload);
    }
}
