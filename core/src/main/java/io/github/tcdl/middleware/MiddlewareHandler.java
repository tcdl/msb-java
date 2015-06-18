package io.github.tcdl.middleware;

import io.github.tcdl.Responder;
import io.github.tcdl.messages.payload.Payload;

/**
 * Created by rdrozdov-tc on 6/2/15.
 */
public interface MiddlewareHandler {

    void handle(Payload response, Responder responder);

}
