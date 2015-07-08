package io.github.tcdl.api.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.tcdl.api.message.MetaMessage.Builder;
import io.github.tcdl.api.message.payload.Payload;
import org.apache.commons.lang3.Validate;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;

/**
 * {@link Message} contains protocol information used by Msb and also provided Acknowledgement (if message {@link Acknowledge}  property is set)
 * and Payload (if message {@link Payload}  property is set).
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
    private final Payload payload;

    @JsonCreator
    private Message(@JsonProperty("id") String id, @JsonProperty("correlationId") String correlationId, @JsonProperty("topics") Topics topics,
            @JsonProperty("meta") MetaMessage meta, @JsonProperty("ack") Acknowledge ack, @JsonProperty("payload") Payload payload) {
        Validate.notNull(id, "the 'id' must not be null");
        Validate.notNull(correlationId, "the 'correlationId' must not be null");
        Validate.notNull(topics, "the 'topics' must not be null");
        Validate.notNull(meta, "the 'meta' must not be null");
        this.id = id;
        this.correlationId = correlationId;
        this.topics = topics;
        this.meta = meta;
        this.ack = ack;
        this.payload = payload;
    }

    public static class Builder {

        private String id;
        private String correlationId;
        private Topics topics;
        private MetaMessage.Builder metaBuilder;
        private Acknowledge ack;
        private Payload payload;

        public Builder setId(String id) {
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

        public Builder withPayload(Payload payload) {
            this.payload = payload;
            return this;
        }

        public Message build() {
            return new Message(id, correlationId, topics, metaBuilder.build(), ack, payload);
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

    public Payload getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message [id=" + id + ", topics=" + topics + ", meta=" + meta + ", ack=" + ack + ", payload=" + payload
                + ", correlationId=" + correlationId + "]";
    }
}
