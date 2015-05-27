package io.github.tcdl;

import io.github.tcdl.config.MsbMessageOptions;
import io.github.tcdl.events.TwoArgsEventHandler;
import io.github.tcdl.messages.Acknowledge.AcknowledgeBuilder;
import io.github.tcdl.messages.Message;
import io.github.tcdl.messages.Message.MessageBuilder;
import io.github.tcdl.messages.MessageFactory;
import io.github.tcdl.messages.MetaMessage.MetaMessageBuilder;
import io.github.tcdl.messages.payload.Payload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rdro on 4/29/2015.
 */
public class Responder {
    
    public static final Logger LOG = LoggerFactory.getLogger(Responder.class);

    private MsbMessageOptions msgOptions;
    private MetaMessageBuilder metaBuilder;
    private AcknowledgeBuilder ackBuilder;
    private Message originalMessage;
    private ChannelManager channelManager;
    private MessageFactory messageFactory;
    private Message responseMessage;

    public Responder(MsbMessageOptions msgOptions, Message originalMessage, MsbContext msbContext) {
        this.msgOptions = msgOptions;
        this.messageFactory = msbContext.getMessageFactory();
        this.metaBuilder = this.messageFactory.createMeta(this.msgOptions);
        this.ackBuilder = this.messageFactory.createAck();
        this.originalMessage = originalMessage;
        this.channelManager = msbContext.getChannelManager();
    }

    public void sendAck(Integer timeoutMs, Integer responsesRemaining,
            TwoArgsEventHandler<Message, Exception> callback) {
        this.ackBuilder.setTimeoutMs(timeoutMs != null && timeoutMs > -1 ? timeoutMs : null);
        this.ackBuilder.setResponsesRemaining(responsesRemaining == null ? 1 : responsesRemaining);

        MessageBuilder ackMessage = this.messageFactory.createAckMessage(originalMessage, ackBuilder.build());
        sendMessage(ackMessage, callback);
    }

    public void send(Payload responsePayload, TwoArgsEventHandler<Message, Exception> callback) {
        this.ackBuilder.setResponsesRemaining(-1);
        MessageBuilder message = this.messageFactory.createResponseMessage(originalMessage, ackBuilder.build(), responsePayload);
        sendMessage(message, callback);
    }

    private void sendMessage(MessageBuilder message, TwoArgsEventHandler<Message, Exception> callback) {
        this.responseMessage = this.messageFactory.completeMeta(message, metaBuilder);

        Producer producer = channelManager.findOrCreateProducer(responseMessage.getTopics().getTo());
        LOG.debug("Publishing message to topic : {}", responseMessage.getTopics().getTo());
        producer.publish(responseMessage, callback);
    }

    public Message getOriginalMessage() {
        return this.originalMessage;
    }

    Message getResponseMessage() {
        return responseMessage;
    }
    
}
