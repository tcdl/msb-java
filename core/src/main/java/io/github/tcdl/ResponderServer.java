package io.github.tcdl;

import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/29/2015.
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
     * Create a new instance of a ResponderServer
     *
     * @param namespace topic to listen for requests
     * @param messageTemplate
     * @param msbContext
     * @param requestHandler handler to be process the request
     * @return new instance of a ResponderServer
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

    public interface RequestHandler {
        void process(Payload request, Responder responder) throws Exception;
    }

    protected void onResponder(Responder responder) {
        Message originalMessage = responder.getOriginalMessage();
        Payload request = originalMessage.getPayload();
        LOG.debug("Pushing message with id {} to middleware chain", originalMessage.getId());

        try {
            requestHandler.process(request, responder);
        } catch (Exception exception) {
            errorHandler(request, responder, exception);
        }
    }

    protected void errorHandler(Payload request, Responder responder, Exception exception) {
        Message originalMessage = responder.getOriginalMessage();
        LOG.error("Handling error for message with id {}", originalMessage.getId());
        Payload responsePayload = new Payload.PayloadBuilder()
                .setStatusCode(500)
                .setStatusMessage(exception.getMessage()).build();
        responder.send(responsePayload);
    }
}
