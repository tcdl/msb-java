package io.github.tcdl;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 4/29/2015.
 */
public class Response {

    private Responder responder;

    public Response(Responder responder) {
        this.responder = responder;
    }

    public Responder getResponder() {
        return responder;
    }
}
