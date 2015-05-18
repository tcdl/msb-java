package io.github.tcdl.messages;

import io.github.tcdl.messages.payload.BasicPayload;

/**
 * Created by rdro on 4/22/2015.
 */
public class Message extends IncommingMessage {

    private String id;// This identifies this message
    private Topics topics = new Topics();
    private MetaMessage meta; // To be filled with createMeta() ->completeMeta() sequence
    private Acknowledge ack; // To be filled on ack or response
    private BasicPayload<?> payload;

    public Message withId(String id) {
        this.id = id;
        return this;
    }

    public Message withCorrelationId(String correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public Message withTopics(Topics topics) {
        this.topics = topics;
        return this;
    }

    public Message withMeta(MetaMessage meta) {
        this.meta = meta;
        return this;
    }

    public Message withAck(Acknowledge ack) {
        this.ack = ack;
        return this;
    }

    public Message withPayload(BasicPayload<?> payload) {
        this.payload = payload;
        return this;
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

    public BasicPayload<?> getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Message [id=" + id + ", topics=" + topics + ", meta=" + meta + ", ack=" + ack + ", payload=" + payload
                + ", correlationId=" + correlationId + "]";
    }

}
