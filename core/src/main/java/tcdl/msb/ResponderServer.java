package tcdl.msb;

import tcdl.msb.config.MsbMessageOptions;
import tcdl.msb.events.EventEmitter;
import tcdl.msb.events.EventHandler;
import tcdl.msb.events.SingleArgumentAdapter;
import tcdl.msb.events.ThreeArgumentsAdapter;
import tcdl.msb.messages.payload.BasicPayload;
import tcdl.msb.middleware.Middleware;
import tcdl.msb.middleware.MiddlewareChain;

import java.util.concurrent.CompletableFuture;

/**
 * Created by rdro on 4/29/2015.
 */
public class ResponderServer {

    private MsbMessageOptions messageOptions;
    private EventEmitter eventEmitter;
    private MiddlewareChain middlewareChain = new MiddlewareChain();

    public ResponderServer(MsbMessageOptions messageOptions) {
        this.messageOptions = messageOptions;
    }

    public ResponderServer use(Middleware... middleware) {
        middlewareChain.add(middleware);
        return this;
    }

    public ResponderServer listen() {
        if (eventEmitter != null) {
            throw new IllegalStateException("Already listening");
        }

        eventEmitter = Responder.createEmitter(this.messageOptions);
        eventEmitter.on(Responder.RESPONDER_EVENT, onResponder);

        return this;
    };

    private EventHandler onResponder = new SingleArgumentAdapter<Responder>() {
        @Override
        public void onEvent(Responder responder) {
            BasicPayload request = responder.getOriginalMessage().getPayload();
            Response response = new Response(responder);
            CompletableFuture.supplyAsync(() ->
                    middlewareChain
                            .withErrorHandler(new ThreeArgumentsAdapter<BasicPayload, Response, Exception>() {
                                @Override
                                public void onEvent(BasicPayload request, Response response, Exception error) {
                                    if (error == null) return;
                                    errorHandler(request, response, error);
                                }
                            })
                            .invoke(request, response));
        }
    };

    private void errorHandler(BasicPayload request, Response response, Exception err) {
        System.out.println("Error 500: " + err.getMessage());
        // TODO write error
    }
}
