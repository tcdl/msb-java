package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.Event;
import io.github.tcdl.events.SingleArgEventHandler;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.middleware.Middleware;
import io.github.tcdl.middleware.MiddlewareChain;
import io.github.tcdl.support.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import static io.github.tcdl.events.Event.RESPONDER_EVENT;

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
//        if (channelManager != null) {
//            throw new IllegalStateException("Already listening");
//        }

        String topic = messageOptions.getNamespace();
        ChannelManager channelManager = msbContext.getChannelManager();

        channelManager.on(Event.MESSAGE_EVENT, (Message message) -> {
            if (Utils.isServiceTopic(message.getTopics().getTo())) {
                /* FIXME this is a hack to prevent consuming of service messages by
                middleware chain.
                Should be removed once this handler will be subscribed to specific topic
                and not on the shared channel manager */
                return;
            }
            Responder responder = new Responder(messageOptions, message, msbContext);
            channelManager.emit(RESPONDER_EVENT, responder);
            onResponder.onEvent(responder);
        });

        channelManager.findOrCreateConsumer(topic);

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
