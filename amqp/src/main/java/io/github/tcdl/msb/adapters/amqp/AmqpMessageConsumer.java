package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Consumer that converts message body to a String before passing it to handler.
 * Also rejects message in case of any exception during its processing to prevent AMQP channel from being closed.
 */
public class AmqpMessageConsumer extends DefaultConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpMessageConsumer.class);

    ConsumerAdapter.RawMessageHandler msgHandler;
    private AmqpBrokerConfig amqpBrokerConfig;

    public AmqpMessageConsumer(Channel channel, ConsumerAdapter.RawMessageHandler msgHandler, AmqpBrokerConfig amqpBrokerConfig) {
        super(channel);
        this.msgHandler = msgHandler;
        this.amqpBrokerConfig = amqpBrokerConfig;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        long deliveryTag = envelope.getDeliveryTag();
        AcknowledgementHandlerInternal ackHandler = createAcknowledgementHandler(
                getChannel(), consumerTag, deliveryTag, envelope.isRedeliver());
        try {
            Charset charset = amqpBrokerConfig.getCharset();

            String bodyStr = new String(body, charset);

            LOG.debug("[consumer tag: {}] Message consumed from broker.", consumerTag);
            LOG.trace("Message: {}", bodyStr);

            try {
                msgHandler.onMessage(bodyStr, ackHandler);
                LOG.debug("[consumer tag: {}] Raw message has been handled.", consumerTag);
                LOG.trace("Message: {}", bodyStr);
            } catch (Exception e) {
                LOG.error("[consumer tag: {}] Can't handle a raw message.", consumerTag);
                LOG.trace("Message: {}.", bodyStr);
                throw e;
            }
        } catch (Exception e) {
            // Catch all exceptions to prevent AMQP channel to be closed
            LOG.error("[consumer tag: {}] Got exception while processing incoming message. About to send AMQP reject...", consumerTag, e);
            ackHandler.autoReject();
        }
    }

    AcknowledgementHandlerInternal createAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag, boolean isRequeueRejectedMessages) {
        AmqpAcknowledgementAdapter adapter = new AmqpAcknowledgementAdapter(channel, consumerTag, deliveryTag);
        String messageTextIdentifier = "consumer tag: " + consumerTag;
        return new AcknowledgementHandlerImpl(adapter, isRequeueRejectedMessages, messageTextIdentifier);
    }
    
}
