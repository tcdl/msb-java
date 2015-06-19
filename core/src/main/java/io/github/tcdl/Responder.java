package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
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
 * {@link Responder} is a component which holds both original request message and response message.
 * Is used by {@link ResponderServer} to send responses and acknowledgements
 *
 * Created by rdro on 4/29/2015.
 */
public class Responder {

    private static final Logger LOG = LoggerFactory.getLogger(Responder.class);

    private String responderId;
    private Message originalMessage;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;
    private Message responseMessage;

    public Responder(MsbMessageOptions config, Message originalMessage, MsbContext msbContext) {
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
     * @param timeoutMs Time to wait for responsesRemaining
     * @param responsesRemaining Expected number of responses
     */
    public void sendAck(Integer timeoutMs, Integer responsesRemaining) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withTimeoutMs(timeoutMs != null && timeoutMs > -1 ? timeoutMs : null);
        ackBuilder.withResponsesRemaining(responsesRemaining == null ? 1 : responsesRemaining);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), null);
        sendMessage(message);
    }

    /**
     * Send payload message.
     */
    public void send(Payload responsePayload) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.withResponderId(responderId);
        ackBuilder.withResponsesRemaining(-1);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), responsePayload);
        sendMessage(message);
    }

    private void sendMessage(Message message) {
        this.responseMessage = message;
        Producer producer = channelManager.findOrCreateProducer(message.getTopics().getTo());
        LOG.debug("Publishing message to topic : {}", message.getTopics().getTo());
        producer.publish(message);
    }

    private void validateReceivedMessage(Message originalMessage) {
        Validate.notNull(originalMessage, "the 'originalMessage' must not be null");
        Validate.notNull(originalMessage.getTopics(), "the 'originalMessage.topics' must not be null");
    }

    public Message getOriginalMessage() {
        return this.originalMessage;
    }

    Message getResponseMessage() {
        return responseMessage;
    }

}
