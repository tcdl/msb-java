package io.github.tcdl.middleware;

import io.github.tcdl.Responder;
import io.github.tcdl.messages.payload.Payload;

/**
 * Created by rdro on 4/29/2015.
 */
public interface Middleware {

    void execute(Payload request, Responder responder) throws Exception;

    default void execute(Payload request, Responder responder, MiddlewareChain chain) throws Exception {
        execute(request, responder);
        chain.execute(request, responder);
    }
}
