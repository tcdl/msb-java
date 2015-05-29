package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Created by rdro on 4/29/2015.
 */
public class ResponderServer {

    public static final Logger LOG = LoggerFactory.getLogger(ResponderServer.class);

    private MsbContext msbContext;
    private MsbMessageOptions messageOptions;
    
    private MiddlewareChain middlewareChain = new MiddlewareChain();

    private ResponderServer(MsbMessageOptions messageOptions, MsbContext msbContext) {
        this.messageOptions = messageOptions;
        this.msbContext = msbContext;
    }

    public static ResponderServer create(MsbMessageOptions msgOptions, MsbContext msbContext) {
        return new ResponderServer(msgOptions, msbContext);
    }

    public ResponderServer use(Middleware... middleware) {
        middlewareChain.add(middleware);
        return this;
    }

    public ResponderServer listen() {
        String topic = messageOptions.getNamespace();
        ChannelManager channelManager = msbContext.getChannelManager();

        Consumer consumer = channelManager.findOrCreateConsumer(topic);

        consumer.on(Event.MESSAGE_EVENT, (Message message) -> {
            Responder responder = new Responder(messageOptions, message, msbContext);
            onResponder(responder);
        });

        return this;
    }

    protected void onResponder (Responder responder) {
            Payload request = responder.getOriginalMessage().getPayload();      
            CompletableFuture.supplyAsync(() ->
                    middlewareChain
                            .withErrorHandler((req, resp, error) -> {
                                    if (error == null)
                                        return;
                                    errorHandler(request, responder, error);
                            })
                            .invoke(request, responder));
    };

    private void errorHandler(Payload request, Responder responder, Exception err) {
        LOG.error("Error processing request {}", request);
        Payload responsePayload = new Payload.PayloadBuilder()
                .setStatusCode(500)
                .setStatusMessage(err.getMessage()).build();
        responder.send(responsePayload, null);
    }
}
