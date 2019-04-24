package io.github.tcdl.msb.adapters.activemq;

import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

public class ActiveMQMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQMessageConsumer.class);

    private ConsumerAdapter.RawMessageHandler msgHandler;

    public ActiveMQMessageConsumer(ConsumerAdapter.RawMessageHandler msgHandler) {
        this.msgHandler = msgHandler;
    }

    public void handlerMessage(Message message) {
        String messageId = null;
        try {
            messageId = message.getJMSMessageID();
        } catch (JMSException e) {
            LOG.error("Got exception while extracting message id", e);
        }

        AcknowledgementHandlerInternal ackHandler = createAcknowledgementHandler(messageId, message);

        if (!(message instanceof TextMessage)) {
            LOG.error("Unsupported message type {}", message.getClass());
            ackHandler.confirmMessage();
        }

        try {
            String messageBody = ((TextMessage) message).getText();
            LOG.debug("[consumer tag: {}] Message consumed from broker.", messageId);
            LOG.trace("Message: {}", messageBody);

            try {
                msgHandler.onMessage(messageBody, ackHandler);
                LOG.debug("[consumer tag: {}] Raw message has been handled.", messageId);
                LOG.trace("Message: {}", messageBody);
            } catch (Exception e) {
                LOG.error("[consumer tag: {}] Can't handle a raw message.", messageId, e);
                LOG.trace("Message: {}", messageBody);
                throw e;
            }
        } catch (Exception e) {
            LOG.error("[consumer tag: {}] Got exception while processing incoming message. About to send ActiveMQ reject...", messageId, e);
            ackHandler.autoReject();
        }
    }

    private AcknowledgementHandlerInternal createAcknowledgementHandler(String messageId, Message message) {
        ActiveMQAcknowledgementAdapter adapter = new ActiveMQAcknowledgementAdapter(message);
        return new AcknowledgementHandlerImpl(adapter, false, messageId);
    }
}