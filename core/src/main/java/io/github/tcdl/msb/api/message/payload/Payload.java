package io.github.tcdl.msb.api.message.payload;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PROTECTED_AND_PUBLIC;
import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * REST-like message payload.
 *
 * @param <Q> class for query
 * @param <H> class for headers
 * @param <P> class for parameters
 * @param <B> class for body
 */
// Workaround to using setFieldVisibility on object mapper. It caused infinite recursion
@JsonAutoDetect(getterVisibility = PROTECTED_AND_PUBLIC, setterVisibility = PROTECTED_AND_PUBLIC)
public class Payload<Q, H, P, B> {

    /**
     * Response status code
     */
    private Integer statusCode;

    /**
     * Response status message
     */
    private String statusMessage;

    /**
     * Query provides instructions. (How)
     */
    private Q query;

    /**
     * Provide things like authorisation, information about the body and information about the user. (Request Meta Info/Who)
     */
    private H headers;

    /**
     * Params provide hierarchical ids of the entities acted upon. (What)
     */
    private P params;

    /**
     * Body provides data/state
     */
    private B body;

    /**
     * Base64-encoded binary body
     */
    private byte[] bodyBuffer;

    protected Payload() {
    }

    @JsonCreator
    private Payload(
            @JsonProperty("statusCode") Integer statusCode,
            @JsonProperty("statusMessage") String statusMessage,
            @JsonProperty("query") Q query,
            @JsonProperty("headers") H headers,
            @JsonProperty("params") P params,
            @JsonProperty("body") B body,
            @JsonProperty("bodyBuffer") byte[] bodyBuffer) {
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

    public Q getQuery() {
        return query;
    }

    protected void setQuery(Q query) {
        this.query = query;
    }

    public H getHeaders() {
        return headers;
    }

    protected void setHeaders(H headers) {
        this.headers = headers;
    }

    public P getParams() {
        return params;
    }

    protected void setParams(P params) {
        this.params = params;
    }

    public B getBody() {
        return body;
    }

    protected void setBody(B body) {
        this.body = body;
    }

    public byte[] getBodyBuffer() {
        return bodyBuffer;
    }

    protected void setBodyBuffer(byte[] bodyBuffer) {
        this.bodyBuffer = bodyBuffer;
    }

    public static class Builder<Q, H, P, B> {

        private Integer statusCode;
        private String statusMessage;
        private Q query;
        private H headers;
        private P params;
        private B body;
        private byte[] bodyBuffer;

        public Builder<Q, H, P, B> withStatusCode(Integer statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder<Q, H, P, B> withStatusMessage(String statusMessage) {
            this.statusMessage = statusMessage;
            return this;
        }

        public Builder<Q, H, P, B> withQuery(Q query) {
            this.query = query;
            return this;
        }

        public Builder<Q, H, P, B> withHeaders(H headers) {
            this.headers = headers;
            return this;
        }

        public Builder<Q, H, P, B> withParams(P params) {
            this.params = params;
            return this;
        }

        public Builder<Q, H, P, B> withBody(B body) {
            this.body = body;
            return this;
        }

        public Builder<Q, H, P, B> withBodyBuffer(byte[] bodyBuffer) {
            this.bodyBuffer = bodyBuffer;
            return this;
        }

        public Payload<Q, H, P, B> build() {
            return new Payload<>(statusCode, statusMessage, query, headers, params, body, bodyBuffer);
        }
    }

    @Override
    public String toString() {
        return String.format("Payload [statusCode=%s, statusMessage=%s, query=%s, headers=%s, params=%s, body=%s, bodyBuffer=%s]",
                statusCode, statusMessage, query, headers, params, body, Arrays.toString(bodyBuffer));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Payload) {
            Payload<?, ?, ?, ?> other = (Payload<?, ?, ?, ?>) obj;
            return Objects.equals(body, other.body)
                    && Objects.deepEquals(bodyBuffer, other.bodyBuffer)
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
