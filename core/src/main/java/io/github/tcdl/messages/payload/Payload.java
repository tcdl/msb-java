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
    private final Map<?, ?> query;
    private final Map<?, ?> params;
    private final Map<?, ?> body;
    private final Map<?, ?> bodyBuffer;

    @JsonCreator
    private Payload(
            @JsonProperty("statusCode") Integer statusCode,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("headers") Map<String, String> headers,
            @JsonProperty("query") Map<?, ?> query,
            @JsonProperty("params") Map<?, ?> params,
            @JsonProperty("body") Map<?, ?> body,
            @JsonProperty("bodyBuffer") Map<?, ?> bodyBuffer) {
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
            try {
                Map queryMap = Utils.fromJson(Utils.toJson(query), Map.class);
                Map paramsMap = Utils.fromJson(Utils.toJson(params), Map.class);
                Map bodyMap = Utils.fromJson(Utils.toJson(body), Map.class);
                Map bodyBufferMap = Utils.fromJson(Utils.toJson(bodyBuffer), Map.class);
                return new Payload(statusCode, statusMessage, headers, queryMap, paramsMap, bodyMap, bodyBufferMap);
            } catch(JsonConversionException e) {
                throw new MessageBuilderException(e.getMessage());
            }
        }
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public Map<?, ?> getHeaders() {
        return headers;
    }

    public Map<?, ?> getQuery() {
        return query;
    }

    public <T> T getQueryAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(query), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public Map<?, ?> getParams() {
        return params;
    }

    public <T> T getParamsAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(params), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public Map<?, ?> getBody() {
        return body;
    }

    public <T> T getBodyAs(Class<T> clazz) {
        try {
            return Utils.fromJson(Utils.toJson(body), clazz);
        } catch(JsonConversionException e) {
            throw new MessageBuilderException(e.getMessage());
        }
    }

    public Map<?, ?> getBodyBuffer() {
        return bodyBuffer;
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
