package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerImpl;
import io.github.tcdl.msb.acknowledge.AcknowledgementHandlerInternal;
import io.github.tcdl.msb.config.amqp.AmqpBrokerConfig;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Special consumer that allows to process messages coming from single AMQP channel in parallel.
 *
 * REASON:
 *
 * AMQP library is implemented in such way that messages that arrive into a single channel are dispatched in a synchronous loop to the consumers and hence
 * will not be processed in parallel.
 *
 * To address this issue {@link AmqpMessageConsumer} just takes incoming message, wraps it in a task and puts into a thread pool. So the actual
 * processing is happening in the separate thread from that thread pool.
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

            LOG.debug(String.format("[consumer tag: %s] Message consumed from broker: %s", consumerTag, bodyStr));

            try {
                msgHandler.onMessage(bodyStr, ackHandler);
                LOG.debug(String.format("[consumer tag: %s] Raw message has been handled: %s.",
                        consumerTag, bodyStr));
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Can't handle a raw message: %s.",
                        consumerTag, bodyStr), e);
                throw e;
            }
        } catch (Exception e) {
            // Catch all exceptions to prevent AMQP channel to be closed
            LOG.error(String.format("[consumer tag: %s] Got exception while processing incoming message. About to send AMQP reject...", consumerTag), e);
            ackHandler.autoReject();
        }
    }

    AcknowledgementHandlerInternal createAcknowledgementHandler(Channel channel, String consumerTag, long deliveryTag, boolean isRequeueRejectedMessages) {
        AmqpAcknowledgementAdapter adapter = new AmqpAcknowledgementAdapter(channel, consumerTag, deliveryTag);
        String messageTextIdentifier = String.format("consumer tag: %s", consumerTag);
        return new AcknowledgementHandlerImpl(adapter, isRequeueRejectedMessages, messageTextIdentifier);
    }
    
}
