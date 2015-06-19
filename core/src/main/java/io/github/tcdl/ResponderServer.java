package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
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

    private MsbContext msbContext;
    private MsbMessageOptions messageOptions;
    private RequestHandler requestHandler;

    private ResponderServer(MsbMessageOptions messageOptions, MsbContext msbContext, RequestHandler requestHandler) {
        this.messageOptions = messageOptions;
        this.msbContext = msbContext;
        this.requestHandler = requestHandler;
        Validate.notNull(requestHandler, "requestHandler must not be null");
    }

    /**
     * Create a new instance of a ResponderServer
     *
     * @param msgOptions
     * @param msbContext
     * @param requestHandler handler to be process the request
     * @return new instance of a ResponderServer
     */

    public static ResponderServer create(MsbMessageOptions msgOptions, MsbContext msbContext, RequestHandler requestHandler) {
        return new ResponderServer(msgOptions, msbContext, requestHandler);
    }

    /**
     * Start listening for message on specified topic.
     */
    public ResponderServer listen() {
        String topic = messageOptions.getNamespace();
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.subscribe(topic, new Consumer.Subscriber() {
                    @Override
                    public void handleMessage(Message message) {
                        LOG.debug("Received message with id {} from topic {}", message.getId(), topic);
                        Responder responder = new Responder(messageOptions, message, msbContext);
                        onResponder(responder);
                    }

                    @Override
                    public void handleError(Exception exception) {
                        LOG.debug("Received exception {}", exception);
                        Responder responder = new Responder(messageOptions, null, msbContext);
                        errorHandler(null, responder, exception);
                    }
                }
        );

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
