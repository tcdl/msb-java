package io.github.tcdl.api.message.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.tcdl.api.exception.JsonConversionException;
import io.github.tcdl.api.exception.MessageBuilderException;
import io.github.tcdl.support.Utils;

/**
 * REST-like message payload.
 */
public final class Payload {

    /**
     * Response status code
     */
    private Integer statusCode;

    /**
     * Response status message
     */
    private String statusMessage;

    /**
     * Provide things like authorisation, information about the body and information about the user. (Request Meta Info/Who)
     */
    private Object headers;

    /**
     * Query provides instructions. (How)
     */
    private Object query;

    /**
     * Params provide hierarchical ids of the entities acted upon. (What)
     */
    private Object params;

    /**
     * Body provides data/state
     */
    private Object body;

    /**
     * Base64-encoded binary body
     */
    private Object bodyBuffer;

    @JsonCreator
    private Payload(
            @JsonProperty("statusCode") Integer statusCode,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("headers") Object headers,
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

    public static class Builder {

        private Integer statusCode;
        private String statusMessage;
        private Object headers;
        private Object query;
        private Object params;
        private Object body;
        private Object bodyBuffer;

        public Builder withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public Builder withHeaders(Object headers) {
            this.headers = headers;
            return this;
        }

        public Builder withQuery(Object query) {
            this.query = query;
            return this;
        }

        public Builder withParams(Object params) {
            this.params = params;
            return this;
        }

        public Builder withBody(Object body) {
            this.body = body;
            return this;
        }

        public Builder withBodyBuffer(Object bodyBuffer) {
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

    public Object getHeaders() {
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
