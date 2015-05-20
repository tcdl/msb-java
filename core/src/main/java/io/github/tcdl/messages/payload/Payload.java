package io.github.tcdl.messages.payload;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by rdro on 4/23/2015.
 */
public final class Payload {

    private final Map<String, String> headers;
    private final Map<?, ?> body;
    private final Map<?, ?> bodyBuffer;

    @JsonCreator
    private Payload(@JsonProperty("headers") Map<String, String> headers, @JsonProperty("body") Map<?, ?> body, @JsonProperty("bodyBuffer") Map<?, ?> bodyBuffer) {
        this.headers = headers;
        this.body = body;
        this.bodyBuffer = bodyBuffer;
    }

    public static class PayloadBuilder {

        private Map<String, String> headers; 
        private Map<?, ?> body;
        private Map<?, ?> bodyBuffer;

        public PayloadBuilder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public PayloadBuilder setBody(Map<?, ?> body) {
            this.body = body;
            return this;
        }

        public PayloadBuilder setBodyBuffer(Map<?, ?> bodyBuffer) {
            this.bodyBuffer = bodyBuffer;
            return this;
        }

        public Payload build() {
            return new Payload(headers, body, bodyBuffer);
        }
    }

    public Map<?, ?> getHeaders() {
        return headers;
    }

    public Map<?, ?> getBody() {
        return body;
    }

    @Override
    public String toString() {
        return String.format("Payload [headers=%s, body=%s, bodyBuffer=%s]", headers, body, bodyBuffer);
    }
}
