package io.github.tcdl.messages;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.commons.lang3.Validate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.tcdl.messages.payload.Payload;

/**
 * Created by rdro on 4/22/2015.
 */
public final class Message {

    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String id;// This identifies this message
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final String correlationId;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final Topics topics;
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final MetaMessage meta; // To be filled with createMeta() ->completeMeta() sequence
    @JsonInclude(JsonInclude.Include.ALWAYS)
    private final Acknowledge ack; // To be filled on ack or response
    @JsonInclude(JsonInclude.Include.ALWAYS)
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

    public static class MessageBuilder {

        private String id;
        private String correlationId;
        private Topics topics;
        private MetaMessage meta;
        private Acknowledge ack;
        private Payload payload;

        public MessageBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public MessageBuilder setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public MessageBuilder setTopics(Topics topics) {
            this.topics = topics;
            return this;
        }

        public MessageBuilder setMeta(MetaMessage meta) {
            this.meta = meta;
            return this;
        }

        public MessageBuilder setAck(Acknowledge ack) {
            this.ack = ack;
            return this;
        }

        public MessageBuilder setPayload(Payload payload) {
            this.payload = payload;
            return this;
        }

        public Message build() {
            return new Message(id, correlationId, topics, meta, ack, payload);
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
