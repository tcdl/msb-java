package io.github.tcdl;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rdro on 4/29/2015.
 */
public class Response {
    private Map<?, ?> body = new HashMap<>();

    public void setBody(Map<?, ?> body) {
        this.body = body;
    }

    public Map<?, ?> getBody() {
        return this.body;
    }

    private Responder responder;

    public Response(Responder responder) {
        this.responder = responder;
    }

    public Responder getResponder() {
        return responder;
    }
}
