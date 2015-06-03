package io.github.tcdl.messages;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.config.ServiceDetails;
import io.github.tcdl.messages.Acknowledge.AcknowledgeBuilder;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;

import javax.annotation.Nullable;
import java.time.Clock;

/**
 * Created by rdro on 4/22/2015.
 */
public class MessageFactory {

    private ServiceDetails serviceDetails;
    private Clock clock;

    public MessageFactory(ServiceDetails serviceDetails, Clock clock) {
        Validate.notNull(serviceDetails, "'serviceDetails' must not be null");
        Validate.notNull(clock, "'clock' must not be null");
        this.serviceDetails = serviceDetails;
        this.clock = clock;
    }

    public MessageBuilder createRequestMessageBuilder(MsbMessageOptions config, Message originalMessage) {
        MessageBuilder messageBuilder = createMessageBuilderWithMeta(config, originalMessage);
        Topics topic = new Topics.TopicsBuilder().setTo(config.getNamespace())
                .setResponse(config.getNamespace() + ":response:" + this.serviceDetails.getInstanceId()).build();
        return messageBuilder.setTopics(topic);
    }

    public MessageBuilder createResponseMessageBuilder(MsbMessageOptions config, Message originalMessage) {
        MessageBuilder messageBuilder = createMessageBuilderWithMeta(config, originalMessage);
        Topics topic = new Topics.TopicsBuilder().setTo(originalMessage.getTopics().getResponse()).build();
        return messageBuilder.setTopics(topic);
    }

    private MessageBuilder createMessageBuilderWithMeta(MsbMessageOptions config, Message originalMessage) {
        MessageBuilder messageBuilder = createBaseMessage(originalMessage);
        MetaMessageBuilder metaBuilder = createMetaBuilder(config);
        return messageBuilder.setMetaBuilder(metaBuilder);
    }

    public Message createRequestMessage(MessageBuilder messageBuilder, Payload payload) {
        if (payload != null) {
            messageBuilder.setPayload(payload);
        }
        return messageBuilder.build();
    }

    public Message createResponseMessage(MessageBuilder messageBuilder, Acknowledge ack, Payload payload) {
        messageBuilder.setPayload(payload);
        messageBuilder.setAck(ack);
        return messageBuilder.build();
    }

    public AcknowledgeBuilder createAckBuilder() {
        return new Acknowledge.AcknowledgeBuilder().setResponderId(Utils.generateId());
    }

    public MetaMessageBuilder createMetaBuilder(MsbMessageOptions config) {
        Integer ttl = config == null ? null : config.getTtl();
        return new MetaMessage.MetaMessageBuilder(ttl, clock.instant(), this.serviceDetails, clock);
    }

    public MessageBuilder createBroadcastMessageBuilder(MsbMessageOptions config, String topicTo, Payload payload) {
        MessageBuilder messageBuilder = createBaseMessage(null);
        MetaMessageBuilder metaBuilder = createMetaBuilder(config);
        messageBuilder.setMetaBuilder(metaBuilder);

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
}
