package io.github.tcdl.msb.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.message.Acknowledge;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.api.message.MetaMessage;
import io.github.tcdl.msb.api.message.MetaMessage.Builder;
import io.github.tcdl.msb.api.message.Topics;
import io.github.tcdl.msb.config.ServiceDetails;
import io.github.tcdl.msb.support.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MessageFactory {

    private ServiceDetails serviceDetails;
    private Clock clock;
    private ObjectMapper payloadMapper;

    public MessageFactory(ServiceDetails serviceDetails, Clock clock, ObjectMapper payloadMapper) {
        Validate.notNull(serviceDetails, "'serviceDetails' must not be null");
        Validate.notNull(clock, "'clock' must not be null");
        Validate.notNull(clock, "'payloadMapper' must not be null");
        this.serviceDetails = serviceDetails;
        this.clock = clock;
        this.payloadMapper = payloadMapper;
    }

    public Message createRequestMessage(Message.Builder messageBuilder, Object payload) {
        JsonNode convertedPayload = Utils.convert(payload, JsonNode.class, payloadMapper);
        messageBuilder.withPayload(convertedPayload);
        return messageBuilder.build();
    }

    public Message createResponseMessage(Message.Builder messageBuilder, Acknowledge ack, Object payload) {
        JsonNode convertedPayload = Utils.convert(payload, JsonNode.class, payloadMapper);
        messageBuilder.withPayload(convertedPayload);
        messageBuilder.withAck(ack);
        return messageBuilder.build();
    }

    public Message createBroadcastMessage(Message.Builder messageBuilder, Object payload) {
        return createRequestMessage(messageBuilder, payload);
    }

    public Message.Builder createRequestMessageBuilder(String namespace, String forwardNamespace, String routingKey, MessageTemplate messageTemplate, Message originalMessage) {
        String responseNamespace = StringUtils.isBlank(forwardNamespace)
                ? namespace + ":response:" + this.serviceDetails.getInstanceId()
                : null;

        Topics topic = new Topics(namespace, responseNamespace, forwardNamespace, routingKey);
        return createMessageBuilder(topic, messageTemplate, originalMessage, false);
    }

    public Message.Builder createRequestMessageBuilder(String namespace, String forwardNamespace, MessageTemplate messageTemplate, Message originalMessage) {
        return createRequestMessageBuilder(namespace, forwardNamespace, null, messageTemplate, originalMessage);
    }

    public Message.Builder createResponseMessageBuilder(MessageTemplate messageTemplate, Message originalMessage) {
        Topics topic = new Topics(originalMessage.getTopics().getResponse(), null, null);
        return createMessageBuilder(topic, messageTemplate, originalMessage, true);
    }

    public Message.Builder createBroadcastMessageBuilder(String namespace, MessageTemplate messageTemplate) {
        Topics topic = new Topics(namespace, null, null);
        return createMessageBuilder(topic, messageTemplate, null, false);
    }

    public Acknowledge.Builder createAckBuilder() {
        return new Acknowledge.Builder().withResponderId(Utils.generateId());
    }

    private Message.Builder createMessageBuilder(Topics topics, MessageTemplate messageTemplate, Message originalMessage, boolean isResponseMessage) {
        Message.Builder messageBuilder = new Message.Builder().withId(Utils.generateId());
        messageBuilder.withTags(createTags(messageTemplate, originalMessage));
        messageBuilder.withTopics(topics);
        messageBuilder.withMetaBuilder(createMetaBuilder(messageTemplate));
        if (isResponseMessage) {
            messageBuilder.withCorrelationId(createCorrelationId(originalMessage));
        } else {
            messageBuilder.withCorrelationId(createCorrelationId(null));
        }
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

    private List<String> createTags(MessageTemplate messageTemplate, Message originalMessage) {
        List<String> tags = new ArrayList<>();
        if (originalMessage != null
                && originalMessage.getTags() != null
                && !originalMessage.getTags().isEmpty()) {
            tags.addAll(originalMessage.getTags());
        }

        if (messageTemplate != null
                && messageTemplate.getTags() != null
                && !messageTemplate.getTags().isEmpty()) {
            tags.addAll(messageTemplate.getTags());
        }

        return tags.stream().distinct().collect(Collectors.toList());
    }
}
