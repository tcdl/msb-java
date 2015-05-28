package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Acknowledge.AcknowledgeBuilder;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.payload.Payload;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/29/2015.
 */
public class Responder {

    public static final Logger LOG = LoggerFactory.getLogger(Responder.class);

    private Message originalMessage;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private MessageBuilder messageBuilder;
    private Message responseMessage;

    public Responder(MsbMessageOptions config, Message originalMessage, MsbContext msbContext) {
        validateRecievedMessage(originalMessage);
        this.originalMessage = originalMessage;
        this.channelManager = msbContext.getChannelManager();
        this.messageFactory = msbContext.getMessageFactory();
        this.messageBuilder = messageFactory.createResponseMessageBuilder(config, originalMessage);
    }

    public void sendAck(Integer timeoutMs, Integer responsesRemaining,
            TwoArgsEventHandler<Message, Exception> callback) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.setTimeoutMs(timeoutMs != null && timeoutMs > -1 ? timeoutMs : null);
        ackBuilder.setResponsesRemaining(responsesRemaining == null ? 1 : responsesRemaining);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), null);
        sendMessage(message, callback);
    }

    public void send(Payload responsePayload, TwoArgsEventHandler<Message, Exception> callback) {
        AcknowledgeBuilder ackBuilder = this.messageFactory.createAckBuilder();
        ackBuilder.setResponsesRemaining(-1);

        Message message = this.messageFactory.createResponseMessage(this.messageBuilder, ackBuilder.build(), responsePayload);
        sendMessage(message, callback);
    }

    private void sendMessage(Message message, TwoArgsEventHandler<Message, Exception> callback) {
        this.responseMessage = message;
        Producer producer = channelManager.findOrCreateProducer(message.getTopics().getTo());
        LOG.debug("Publishing message to topic : {}", message.getTopics().getTo());
        producer.publish(message, callback);
    }

    private void validateRecievedMessage(Message originalMessage) {
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
