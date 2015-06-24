package io.github.tcdl.messages.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by rdro on 4/23/2015.
 */
public final class Payload {

    private Integer statusCode;
    private String statusMessage;
    private final Map<String, String> headers;
    private Object query;
    private Object params;
    private Object body;
    private Object bodyBuffer;

    @JsonCreator
    private Payload(
            @JsonProperty("statusCode") Integer statusCode,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("query") Object query,
            @JsonProperty("params") Object params,
            @JsonProperty("body") Object body,
            @JsonProperty("bodyBuffer") Object bodyBuffer) {
        this.statusMessage = statusMessage;
        this.statusCode = statusCode;
        this.headers = headers;
        this.query = query;
        this.params = params;
        this.body = body;
        this.bodyBuffer = bodyBuffer;
    }

    public static class PayloadBuilder {

        private Integer statusCode;
        private String statusMessage;
        private Map<String, String> headers;
        private Object query;
        private Object params;
        private Object body;
        private Object bodyBuffer;

        public PayloadBuilder setStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public PayloadBuilder setStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public PayloadBuilder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public PayloadBuilder setQuery(Object query) {
            this.query = query;
            return this;
        }

        public PayloadBuilder setParams(Object params) {
            this.params = params;
            return this;
        }

        public PayloadBuilder setBody(Object body) {
            this.body = body;
            return this;
        }

        public PayloadBuilder setBodyBuffer(Object bodyBuffer) {
            this.bodyBuffer = bodyBuffer;
            return this;
        }

        public Payload build() {
            return new Payload(statusCode, statusMessage, headers, query, params, body, bodyBuffer);
        }
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @SuppressWarnings("unchecked")
    public <T> T getQuery() {
        return (T)query;
    }

    @SuppressWarnings("unchecked")
    public <T> T getParams() {
        return (T)params;
    }

    @SuppressWarnings("unchecked")
    public <T> T getBody() {
        return (T)body;
    }

    @SuppressWarnings("unchecked")
    public <T> T getBodyBuffer() {
        return (T)bodyBuffer;
    }

    @Override
    public String toString() {
        return String.format("Payload [statusCode=%s, statusMessage=%s, headers=%s, query=%s, params=%s, body=%s, bodyBuffer=%s]",
                statusCode, statusMessage, headers, query, params, body, bodyBuffer);
    }
}
