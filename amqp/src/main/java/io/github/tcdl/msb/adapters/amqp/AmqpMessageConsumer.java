package io.github.tcdl.msb.adapters.amqp;

import io.github.tcdl.msb.adapters.ConsumerAdapter;
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

    ExecutorService consumerThreadPool;
    ConsumerAdapter.RawMessageHandler msgHandler;
    private AmqpBrokerConfig amqpBrokerConfig;

    public AmqpMessageConsumer(Channel channel, ExecutorService consumerThreadPool, ConsumerAdapter.RawMessageHandler msgHandler, AmqpBrokerConfig amqpBrokerConfig) {
        super(channel);
        this.consumerThreadPool = consumerThreadPool;
        this.msgHandler = msgHandler;
        this.amqpBrokerConfig = amqpBrokerConfig;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
            Charset charset = amqpBrokerConfig.getCharset();
            boolean requeueRejectedMessages = amqpBrokerConfig.isRequeueRejectedMessages();
            String bodyStr = new String(body, charset);
            long deliveryTag = envelope.getDeliveryTag();
            
            LOG.debug(String.format("[consumer tag: %s] Message consumed from broker: %s", consumerTag, bodyStr));

            AmqpAcknowledgementHandler ackHandler = createAcknowledgementHandler(getChannel(), 
                    consumerTag, bodyStr, deliveryTag, requeueRejectedMessages);
            try {
                consumerThreadPool.submit(new AmqpMessageProcessingTask(consumerTag, bodyStr, msgHandler, ackHandler));
                LOG.debug(String.format("[consumer tag: %s] Message has been put in the processing queue: %s. About to send AMQP ack...",
                        consumerTag, bodyStr));
            } catch (Exception e) {
                LOG.error(String.format("[consumer tag: %s] Couldn't put message in the processing queue: %s. About to send AMQP reject...",
                        consumerTag, bodyStr), e);
                ackHandler.autoReject();
            }
        } catch (Exception e) {
            // Catch all exceptions to prevent AMQP channel to be closed
            LOG.error(String.format("[consumer tag: %s] Got exception while processing incoming message", consumerTag), e);
        }
    }
    
    AmqpAcknowledgementHandler createAcknowledgementHandler(Channel channel, String consumerTag,
            String bodyStr, long deliveryTag, boolean isRequeueRejectedMessages) {
        return new AmqpAcknowledgementHandler(channel, consumerTag, bodyStr, deliveryTag, isRequeueRejectedMessages);
    }
    
}
