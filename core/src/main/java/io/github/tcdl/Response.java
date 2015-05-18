package io.github.tcdl;

import io.github.tcdl.messages.payload.BasicPayload;

/**
 * Created by rdro on 4/29/2015.
 */
public class Response extends BasicPayload {

    private Responder responder;

    public Response(Responder responder) {
        this.responder = responder;
    }

    public Responder getResponder() {
        return responder;
    }
}
