package io.github.tcdl.msb.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.tcdl.msb.adapters.ConsumerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;

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
    private Charset charset;

    public AmqpMessageConsumer(Channel channel, ExecutorService consumerThreadPool, ConsumerAdapter.RawMessageHandler msgHandler, Charset charset) {
        super(channel);
        this.consumerThreadPool = consumerThreadPool;
        this.msgHandler = msgHandler;
        this.charset = charset;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        try {
            String bodyStr = new String(body, charset);
            LOG.debug(String.format("[consumer tag: %s] Message consumed from broker: %s", consumerTag, bodyStr));
            consumerThreadPool.submit(new AmqpMessageProcessingTask(consumerTag, bodyStr, getChannel(), envelope.getDeliveryTag(), msgHandler));
            LOG.debug(String.format("[consumer tag: %s] Message has been put into processing pool: %s", consumerTag, bodyStr));
        } catch (Exception e) {
            // Catch all exceptions to prevent AMQP channel to be closed
            LOG.error(String.format("[consumer tag: %s] Got exception while processing incoming message", consumerTag), e);
        }
    }
}
