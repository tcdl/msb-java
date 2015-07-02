package io.github.tcdl.impl;

import static io.github.tcdl.api.message.Acknowledge.AcknowledgeBuilder;

import io.github.tcdl.ChannelManager;
import io.github.tcdl.Producer;
import io.github.tcdl.api.MessageTemplate;
import io.github.tcdl.api.MsbContext;
import io.github.tcdl.api.Responder;
import io.github.tcdl.api.message.Message;
import io.github.tcdl.api.message.Message.MessageBuilder;
import io.github.tcdl.api.message.payload.Payload;
import io.github.tcdl.message.MessageFactory;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderImpl implements Responder {

    private static final Logger LOG = LoggerFactory.getLogger(ResponderImpl.class);

    private String responderId;
    private Message originalMessage;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;

    public ResponderImpl(MessageTemplate messageTemplate, Message originalMessage, MsbContext msbContext) {
        validateReceivedMessage(originalMessage);
        this.responderId = Utils.generateId();
        this.originalMessage = originalMessage;
        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.messageBuilder = messageFactory.createResponseMessageBuilder(messageTemplate, originalMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message sendAck(Integer timeoutMs, Integer responsesRemaining) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withTimeoutMs(timeoutMs != null && timeoutMs > -1 ? timeoutMs : null);
        ackBuilder.withResponsesRemaining(responsesRemaining == null ? 1 : responsesRemaining);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), null);
        sendMessage(message);

        return message;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Message send(Payload responsePayload) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withResponsesRemaining(-1);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), responsePayload);
        sendMessage(message);

        return message;
    }

    private void sendMessage(Message message) {
        Producer producer = channelManager.findOrCreateProducer(message.getTopics().getTo());
        LOG.debug("Publishing message to topic : {}", message.getTopics().getTo());
        producer.publish(message);
    }

    private void validateReceivedMessage(Message originalMessage) {
        Validate.notNull(originalMessage, "the 'originalMessage' must not be null");
        Validate.notNull(originalMessage.getTopics(), "the 'originalMessage.topics' must not be null");
    }

    /**
     * @return original message which was received
     */
    public Message getOriginalMessage() {
        return this.originalMessage;
    }
}