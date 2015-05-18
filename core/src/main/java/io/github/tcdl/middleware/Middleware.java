package io.github.tcdl.middleware;

import io.github.tcdl.Response;
import io.github.tcdl.messages.payload.Payload;

/**
 * Created by rdro on 4/29/2015.
 */
public interface Middleware {

    void execute(Payload request, Response response) throws Exception;

    default void execute(Payload request, Response response, MiddlewareChain chain) throws Exception {
        execute(request, response);
        chain.execute(request, response);
    }
}
