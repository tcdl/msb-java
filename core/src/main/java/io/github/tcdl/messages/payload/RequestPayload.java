package io.github.tcdl.messages.payload;

import java.util.HashMap;
import java.util.Map;

public class RequestPayload extends BasicPayload<RequestPayload> {

    private Map<?, ?> params = new HashMap<>();

    public RequestPayload withParams(Map<?, ?> params) {
        this.params = params;
        return this;
    }

    public Map<?, ?> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return "RequestPayload [params=" + params + ", getHeaders()=" + getHeaders() + ", getBody()=" + getBody() + "]";
    }
}
