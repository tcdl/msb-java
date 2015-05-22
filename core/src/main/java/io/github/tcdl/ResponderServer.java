package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.EventEmitter;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.tcdl.events.Event.RESPONDER_EVENT;

/**
 * Created by rdro on 4/29/2015.
 */
public class ResponderServer {

    public static final Logger LOG = LoggerFactory.getLogger(ResponderServer.class);

    private MsbMessageOptions messageOptions;
    private ChannelManager channelManager;
    private MiddlewareChain middlewareChain = new MiddlewareChain();

    private ResponderServer(MsbMessageOptions messageOptions) {
        this.messageOptions = messageOptions;
    }

    public static ResponderServer create(MsbMessageOptions msgOptions) {
        return new ResponderServer(msgOptions);
    }

    public ResponderServer use(Middleware... middleware) {
        middlewareChain.add(middleware);
        return this;
    }

    public ResponderServer listen() {
        if (channelManager != null) {
            throw new IllegalStateException("Already listening");
        }

        String topic = messageOptions.getNamespace();
        channelManager = ChannelManager.getInstance();

        channelManager.on(Event.MESSAGE_EVENT, (Message message) -> {
            Responder responder = new Responder(messageOptions, message);
            channelManager.emit(RESPONDER_EVENT, responder);
            onResponder.onEvent(responder);
        });

        channelManager.findOrCreateConsumer(topic, null);

        return this;
    }

    private SingleArgEventHandler<Responder> onResponder = (Responder responder) -> {
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
