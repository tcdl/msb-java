package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * {@link ResponderServer} component which listens for incoming requests and can send responses.
 *
 * Created by rdro on 4/29/2015.
 */
public class ResponderServer {

    private static final Logger LOG = LoggerFactory.getLogger(ResponderServer.class);

    private MsbContext msbContext;
    private MsbMessageOptions messageOptions;
    
    private MiddlewareChain middlewareChain = new MiddlewareChain();

    private ResponderServer(MsbMessageOptions messageOptions, MsbContext msbContext) {
        this.messageOptions = messageOptions;
        this.msbContext = msbContext;
    }

    /**
     * Create a new instance of a ResponderServer
     *
     * @param msgOptions
     * @param msbContext
     * @return new instance of a ResponderServer
     */
    public static ResponderServer create(MsbMessageOptions msgOptions, MsbContext msbContext) {
        return new ResponderServer(msgOptions, msbContext);
    }

    public ResponderServer use(Middleware... middleware) {
        middlewareChain.add(middleware);
        return this;
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

    protected void onResponder(Responder responder) {
        Message originalMessage = responder.getOriginalMessage();
        Payload request = originalMessage.getPayload();
        LOG.debug("Pushing message with id {} to middleware chain", originalMessage.getId());
        CompletableFuture.supplyAsync(() ->
                middlewareChain
                        .withErrorHandler((req, resp, error) -> {
                            if (error == null)
                                return;
                            errorHandler(request, responder, error);
                        })
                        .invoke(request, responder));
    }

    protected void errorHandler(Payload request, Responder responder, Exception err) {
        Message originalMessage = responder.getOriginalMessage();
        LOG.error("Handling error for message with id {}", originalMessage.getId());
        Payload responsePayload = new Payload.PayloadBuilder()
                .setStatusCode(500)
                .setStatusMessage(err.getMessage()).build();
        responder.send(responsePayload);
    }
}
