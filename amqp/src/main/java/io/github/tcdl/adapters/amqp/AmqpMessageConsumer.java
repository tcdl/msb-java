package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static io.github.tcdl.adapters.ConsumerAdapter.RawMessageHandler;

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
    RawMessageHandler msgHandler;

    public AmqpMessageConsumer(Channel channel, ExecutorService consumerThreadPool, RawMessageHandler msgHandler) {
        super(channel);
        this.consumerThreadPool = consumerThreadPool;
        this.msgHandler = msgHandler;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
            String bodyStr = new String(body);
            LOG.debug(String.format("[consumer tag: %s] Message consumed from broker: %s", consumerTag, bodyStr));
            consumerThreadPool.submit(new AmqpMessageProcessingTask(consumerTag, bodyStr, getChannel(), envelope.getDeliveryTag(), msgHandler));
            LOG.debug(String.format("[consumer tag: %s] Message has been put into processing pool: %s", consumerTag, bodyStr));
        } catch (Exception e) {
            // Catch all exceptions to prevent AMQP channel to be closed
            LOG.error(String.format("[consumer tag: %s] Got exception while processing incoming message", consumerTag), e);
        }
    }
}
