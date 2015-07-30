package io.github.tcdl.msb.message;

import java.time.Clock;

import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.MetaMessage.Builder;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.api.message.payload.Payload;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.Validate;

public class MessageFactory {

    private ServiceDetails serviceDetails;
    private Clock clock;

    public MessageFactory(ServiceDetails serviceDetails, Clock clock) {
        Validate.notNull(serviceDetails, "'serviceDetails' must not be null");
        Validate.notNull(clock, "'clock' must not be null");
        this.serviceDetails = serviceDetails;
        this.clock = clock;
    }

    public Message createRequestMessage(Message.Builder messageBuilder, Payload payload) {
        messageBuilder.withPayload(payload);
        return messageBuilder.build();
    }

    public Message createResponseMessage(Message.Builder messageBuilder, Acknowledge ack, Payload payload) {
        messageBuilder.withPayload(payload);
        messageBuilder.withAck(ack);
        return messageBuilder.build();
    }

    public Message createBroadcastMessage(Message.Builder messageBuilder, Payload payload) {
        messageBuilder.withPayload(payload);
        return messageBuilder.build();
    }

    public Message.Builder createRequestMessageBuilder(String namespace, MessageTemplate messageTemplate, Message originalMessage) {
        Topics topic = new Topics(namespace, namespace + ":response:" +
                this.serviceDetails.getInstanceId());
        return createMessageBuilder(topic, messageTemplate, originalMessage);
    }

    public Message.Builder createResponseMessageBuilder(MessageTemplate messageTemplate, Message originalMessage) {
        Topics topic = new Topics(originalMessage.getTopics().getResponse(), null);
        return createMessageBuilder(topic, messageTemplate, originalMessage);
    }

    public Message.Builder createBroadcastMessageBuilder(String namespace, MessageTemplate messageTemplate) {
        Topics topic = new Topics(namespace, null);
        return createMessageBuilder(topic, messageTemplate, null);
    }

    public Acknowledge.Builder createAckBuilder() {
        return new Acknowledge.Builder().withResponderId(Utils.generateId());
    }

    private Message.Builder createMessageBuilder(Topics topics, MessageTemplate messageTemplate, Message originalMessage) {
        Message.Builder messageBuilder = new Message.Builder().withId(Utils.generateId());
        messageBuilder.withTopics(topics);
        messageBuilder.withMetaBuilder(createMetaBuilder(messageTemplate));
        messageBuilder.withCorrelationId(createCorrelationId(originalMessage));
        return messageBuilder;
    }

    private Builder createMetaBuilder(MessageTemplate config) {
        Integer ttl = (config == null) ? null : config.getTtl();
        return new MetaMessage.Builder(ttl, clock.instant(), this.serviceDetails, clock);
    }

    private String createCorrelationId(Message originalMessage) {
        if (originalMessage != null && originalMessage.getCorrelationId() != null) {
            return originalMessage.getCorrelationId();
        } else {
            return Utils.generateId();
        }
    }
}
