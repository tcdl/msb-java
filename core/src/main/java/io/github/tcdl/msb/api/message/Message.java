package io.github.tcdl.msb.api.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.Validate;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

/**
 * {@link Message} represents a message coming from/to bus. It contains the following information:
 * 1. Protocol information used internally
 * 2. Acknowledgement information
 * 3. "Raw" payload that can be converted to a high-level object at the following processing stages
 */
public final class Message {

    @JsonInclude(ALWAYS)
    private final String id;// This identifies this message
    @JsonInclude(ALWAYS)
    private final String correlationId;
    @JsonInclude(ALWAYS)
    private final Topics topics;
    @JsonInclude(ALWAYS)
    private final MetaMessage meta; // To be filled with createMeta() ->completeMeta() sequence
    @JsonInclude(ALWAYS)
    private final Acknowledge ack; // To be filled on ack or response
    @JsonInclude(ALWAYS)
    @JsonProperty("payload")
    private final JsonNode rawPayload;

    @JsonCreator
    private Message(@JsonProperty("id") String id, @JsonProperty("correlationId") String correlationId, @JsonProperty("topics") Topics topics,
            @JsonProperty("meta") MetaMessage meta, @JsonProperty("ack") Acknowledge ack, @JsonProperty("payload") JsonNode rawPayload) {
        Validate.notNull(id, "the 'id' must not be null");
        Validate.notNull(correlationId, "the 'correlationId' must not be null");
        Validate.notNull(topics, "the 'topics' must not be null");
        Validate.notNull(meta, "the 'meta' must not be null");
        this.id = id;
        this.correlationId = correlationId;
        this.topics = topics;
        this.meta = meta;
        this.ack = ack;
        this.rawPayload = rawPayload;
    }

    public static class Builder {

        private String id;
        private String correlationId;
        private Topics topics;
        private MetaMessage.Builder metaBuilder;
        private Acknowledge ack;
        private JsonNode rawPayload;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder withTopics(Topics topics) {
            this.topics = topics;
            return this;
        }

        public Message.Builder withMetaBuilder(MetaMessage.Builder metaBuilder) {
            this.metaBuilder = metaBuilder;
            return this;
        }

        public Builder withAck(Acknowledge ack) {
            this.ack = ack;
            return this;
        }

        public Builder withPayload(JsonNode rawPayload) {
            this.rawPayload = rawPayload;
            return this;
        }

        public Message build() {
            return new Message(id, correlationId, topics, metaBuilder.build(), ack, rawPayload);
        }
    }

    public String getId() {
        return id;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Topics getTopics() {
        return topics;
    }

    public MetaMessage getMeta() {
        return meta;
    }

    public Acknowledge getAck() {
        return ack;
    }

    public JsonNode getRawPayload() {
        return rawPayload;
    }

    @Override
    public String toString() {
        return String.format("Message [id=%s, topics=%s, meta=%s, ack=%s, rawPayload=%s, correlationId=%s]", id, topics, meta, ack, rawPayload, correlationId);
    }
}
