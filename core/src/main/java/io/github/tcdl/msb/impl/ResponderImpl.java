package io.github.tcdl.msb.impl;

import io.github.tcdl.msb.ChannelManager;
import io.github.tcdl.msb.Producer;
import io.github.tcdl.msb.api.AcknowledgementHandler;
import io.github.tcdl.msb.api.MessageTemplate;
import io.github.tcdl.msb.api.Responder;
import io.github.tcdl.msb.api.message.Acknowledge.Builder;
import io.github.tcdl.msb.api.message.Message;
import io.github.tcdl.msb.message.MessageFactory;
import io.github.tcdl.msb.support.Utils;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponderImpl implements Responder {

    private static final Logger LOG = LoggerFactory.getLogger(ResponderImpl.class);

    private String responderId;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private Message.Builder messageBuilder;

    public ResponderImpl(MessageTemplate messageTemplate, Message originalMessage, 
            MsbContextImpl msbContext) {
        validateReceivedMessage(originalMessage);
        this.responderId = Utils.generateId();
        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.messageBuilder = messageFactory.createResponseMessageBuilder(messageTemplate, originalMessage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendAck(Integer timeoutMs, Integer responsesRemaining) {
        Builder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withTimeoutMs(timeoutMs != null && timeoutMs > -1 ? timeoutMs : null);
        ackBuilder.withResponsesRemaining(responsesRemaining == null ? 1 : responsesRemaining);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), null);
        sendMessage(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void send(Object responsePayload) {
        Builder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withResponsesRemaining(-1);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), responsePayload);
        sendMessage(message);
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

}