package io.github.tcdl.messages.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.tcdl.exception.JsonConversionException;
import io.github.tcdl.exception.MessageBuilderException;
import io.github.tcdl.support.Utils;

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

        public PayloadBuilder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public PayloadBuilder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public PayloadBuilder withHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public PayloadBuilder withQuery(Object query) {
            this.query = query;
            return this;
        }

        public PayloadBuilder withParams(Object params) {
            this.params = params;
            return this;
        }

        public PayloadBuilder withBody(Object body) {
            this.body = body;
            return this;
        }

        public PayloadBuilder withBodyBuffer(Object bodyBuffer) {
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

    public Object getQuery() {
        return query;
    }

    public Object getParams() {
        return params;
    }

    public Object getBody() {
        return body;
    }

    public Object getBodyBuffer() {
        return bodyBuffer;
    }

    public <T> T getQueryAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(query), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getParamsAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(params), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getBodyAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(body), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public <T> T getBodyBufferAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(bodyBuffer), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    @Override
    public String toString() {
        return String.format("Payload [statusCode=%s, statusMessage=%s, headers=%s, query=%s, params=%s, body=%s, bodyBuffer=%s]",
                statusCode, statusMessage, headers, query, params, body, bodyBuffer);
    }
}
