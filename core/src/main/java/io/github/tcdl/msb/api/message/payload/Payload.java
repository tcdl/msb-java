package io.github.tcdl.msb.api.message.payload;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC;

/**
 * REST-like message payload.
 */
// Workaround to using setFieldVisibility on object mapper. It caused infinite recursion
@JsonAutoDetect(getterVisibility = PROTECTED_AND_PUBLIC, setterVisibility = PROTECTED_AND_PUBLIC)
public class Payload implements ConvertiblePayload {

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
    private String bodyBuffer;

    protected Payload() {
    }

    @JsonCreator
    private Payload(
            @JsonProperty("statusCode") Integer statusCode,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("headers") Object headers,
            @JsonProperty("query") Object query,
            @JsonProperty("params") Object params,
            @JsonProperty("body") Object body,
            @JsonProperty("bodyBuffer") String bodyBuffer) {
        this.statusMessage = statusMessage;
        this.statusCode = statusCode;
        this.headers = headers;
        this.query = query;
        this.params = params;
        this.body = body;
        this.bodyBuffer = bodyBuffer;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    protected void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    protected void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public Object getHeaders() {
        return headers;
    }

    protected void setHeaders(Object headers) {
        this.headers = headers;
    }

    public Object getQuery() {
        return query;
    }

    protected void setQuery(Object query) {
        this.query = query;
    }

    public Object getParams() {
        return params;
    }

    protected void setParams(Object params) {
        this.params = params;
    }

    public Object getBody() {
        return body;
    }

    protected void setBody(Object body) {
        this.body = body;
    }

    public String getBodyBuffer() {
        return bodyBuffer;
    }

    protected void setBodyBuffer(String bodyBuffer) {
        this.bodyBuffer = bodyBuffer;
    }

    public static class Builder {

        private Integer statusCode;
        private String statusMessage;
        private Object headers;
        private Object query;
        private Object params;
        private Object body;
        private String bodyBuffer;

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

        public Builder withBodyBuffer(String bodyBuffer) {
            this.bodyBuffer = bodyBuffer;
            return this;
        }

        public Payload build() {
            return new Payload(statusCode, statusMessage, headers, query, params, body, bodyBuffer);
        }
    }

    @Override
    public String toString() {
        return String.format("Payload [statusCode=%s, statusMessage=%s, headers=%s, query=%s, params=%s, body=%s, bodyBuffer=%s]",
                statusCode, statusMessage, headers, query, params, body, bodyBuffer);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Payload) {
            Payload other = (Payload) obj;
            return Objects.equals(body, other.body)
                    && Objects.equals(bodyBuffer, other.bodyBuffer)
                    && Objects.equals(bodyBuffer, other.bodyBuffer)
                    && Objects.equals(headers, other.headers)
                    && Objects.equals(params, other.params)
                    && Objects.equals(query, other.query)
                    && Objects.equals(statusCode, other.statusCode)
                    && Objects.equals(statusMessage, other.statusMessage);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, bodyBuffer, headers, params, query, statusCode, statusMessage);
    }
}
