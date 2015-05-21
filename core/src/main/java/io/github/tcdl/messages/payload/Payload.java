package io.github.tcdl.messages.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by rdro on 4/23/2015.
 */
public final class Payload {

    private final Map<String, String> headers;
    private final Map<?, ?> query;
    private final Map<?, ?> params;
    private final Map<?, ?> body;
    private final Map<?, ?> bodyBuffer;

    @JsonCreator
    private Payload(@JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("query") Map<?, ?> query,
            @JsonProperty("params") Map<?, ?> params,
            @JsonProperty("body") Map<?, ?> body, @JsonProperty("bodyBuffer") Map<?, ?> bodyBuffer) {
        this.headers = headers;
        this.query = query;
        this.params = params;
        this.body = body;
        this.bodyBuffer = bodyBuffer;
    }

    public static class PayloadBuilder {

        private Map<String, String> headers;
        private Map<?, ?> query;
        private Map<?, ?> params;
        private Map<?, ?> body;
        private Map<?, ?> bodyBuffer;

        public PayloadBuilder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public PayloadBuilder setQuery(Map<?, ?> query) {
            this.query = query;
            return this;
        }

        public PayloadBuilder setParams(Map<?, ?> params) {
            this.params = params;
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
            return new Payload(headers, query, params, body, bodyBuffer);
        }
    }

    public Map<?, ?> getHeaders() {
        return headers;
    }

    public Map<?, ?> getQuery() {
        return query;
    }

    public Map<?, ?> getParams() {
        return params;
    }

    public Map<?, ?> getBody() {
        return body;
    }

    @Override
    public String toString() {
        return String.format("Payload [headers=%s, query=%s, params=%s, body=%s, bodyBuffer=%s]", headers, query, params, body, bodyBuffer);
    }
}
