package io.github.tcdl;

import io.github.tcdl.config.MessageTemplate;
import io.github.tcdl.messages.Acknowledge.AcknowledgeBuilder;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;
import io.github.tcdl.support.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Responder} is a component which holds original request message and create response base on it.
 * Is used by {@link ResponderServer} to send responses and acknowledgements.
 */
public class Responder {

    private static final Logger LOG = LoggerFactory.getLogger(Responder.class);

    private String responderId;
    private Message originalMessage;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;

    public Responder(MessageTemplate config, Message originalMessage, MsbContext msbContext) {
        validateReceivedMessage(originalMessage);
        this.responderId = Utils.generateId();
        this.originalMessage = originalMessage;
        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.messageBuilder = messageFactory.createResponseMessageBuilder(config, originalMessage);
    }

    /**
     * Send acknowledge message.
     *
     * @param timeoutMs time to wait for remaining responses
     * @param responsesRemaining expected number of responses
     */
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
     * Send payload message.
     * @param responsePayload payload which will be used to create response message
     * @return response message which was sent
     */
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
     *
     * @return original message which was received
     */
    public Message getOriginalMessage() {
        return this.originalMessage;
    }
}
