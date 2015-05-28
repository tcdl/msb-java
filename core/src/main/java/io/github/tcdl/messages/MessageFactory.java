package io.github.tcdl.messages;

import java.util.Date;

import org.apache.commons.lang3.Validate;

import io.github.tcdl.messages.Acknowledge;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.MetaMessage;
import io.github.tcdl.messages.Topics;
import io.github.tcdl.messages.Acknowledge.AcknowledgeBuilder;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;

import io.github.tcdl.config.MsbConfigurations;
import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.config.ServiceDetails;
import io.github.tcdl.support.Utils;

import javax.annotation.Nullable;

/**
 * Created by rdro on 4/22/2015.
 */
public class MessageFactory {

    private ServiceDetails serviceDetails;

    public MessageFactory(ServiceDetails serviceDetails) {
        this.serviceDetails = serviceDetails;
    }

    public MessageBuilder createRequestMessage(MsbMessageOptions config, Message originalMessage) {
        MessageBuilder messageBuilder = createBaseMessage(originalMessage);
        Topics topic = new Topics.TopicsBuilder().setTo(config.getNamespace())
                .setResponse(config.getNamespace() + ":response:" + this.serviceDetails.getInstanceId()).build();
        return messageBuilder.setTopics(topic);
    }

    public MessageBuilder createResponseMessage(Message originalMessage, Acknowledge ack, Payload payload) {
        validateRecievedMessage(originalMessage);

        MessageBuilder messageBuilder = createBaseMessage(originalMessage);
        messageBuilder.setAck(ack).setPayload(payload);
        return messageBuilder.setTopics(new Topics.TopicsBuilder().setTo(originalMessage.getTopics().getResponse()).build());
    }

    public MessageBuilder createAckMessage(Message originalMessage, Acknowledge ack) {
        validateRecievedMessage(originalMessage);

        MessageBuilder messageBuilder = createBaseMessage(originalMessage);
        messageBuilder.setAck(ack).setPayload(null);
        return messageBuilder.setTopics(new Topics.TopicsBuilder().setTo(originalMessage.getTopics().getResponse()).build());
    }

    public MessageBuilder createBroadcastMessage(String topicTo, Payload payload) {
        MessageBuilder messageBuilder = createBaseMessage(null);
        Topics topics = new Topics.TopicsBuilder().setTo(topicTo).build();

        messageBuilder.setTopics(topics);
        messageBuilder.setPayload(payload);

        return messageBuilder;
    }

    private MessageBuilder createBaseMessage(@Nullable Message originalMessage) {
        MessageBuilder baseMessage = new Message.MessageBuilder()
                .setId(Utils.generateId())
                .setCorrelationId(
                        originalMessage != null && originalMessage.getCorrelationId() != null ? originalMessage
                                .getCorrelationId() : Utils.generateId());

        return baseMessage;
    }

    private void validateRecievedMessage(Message originalMessage) {
        Validate.notNull(originalMessage, "the 'originalMessage' must not be null");
        Validate.notNull(originalMessage.getTopics(), "the 'originalMessage.topics' must not be null");
    }

    public AcknowledgeBuilder createAck() {
        return new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId());
    }

    public MetaMessageBuilder createMeta(MsbMessageOptions config) {
        Integer ttl = config == null ? null : config.getTtl();
        return new MetaMessage.MetaMessageBuilder(ttl, new Date(), this.serviceDetails);
    }

    public Message completeMeta(MessageBuilder message, MetaMessageBuilder meta) {
        meta.computeDurationMs().build();
        return message.setMeta(meta.build()).build();
    }

}
