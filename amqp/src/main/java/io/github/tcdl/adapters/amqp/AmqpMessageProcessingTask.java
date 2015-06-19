package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import io.github.tcdl.adapters.ConsumerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmqpMessageProcessingTask} wraps incoming message. This task is put into message processing thread pool (see {@link AmqpMessageConsumer}).
 */
public class AmqpMessageProcessingTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AmqpMessageProcessingTask.class);

    String consumerTag;
    String body;
    Channel channel;
    long deliveryTag;
    ConsumerAdapter.RawMessageHandler msgHandler;

    public AmqpMessageProcessingTask(String consumerTag, String body, Channel channel, long deliveryTag, ConsumerAdapter.RawMessageHandler msgHandler) {
        this.consumerTag = consumerTag;
        this.body = body;
        this.channel = channel;
        this.deliveryTag = deliveryTag;
        this.msgHandler = msgHandler;
    }

    /**
     * Passes the message to the configured handler and acknowledges it to AMQP broker.
     * IMPORTANT CAVEAT: This task is meant to be run in a thread pool so it should handle all its exceptions carefully. In particular it shouldn't
     * throw an exception (because it's going to be swallowed anyway and not printed)
     */
    @Override
    public void run() {
        try {
            LOG.debug(String.format("[consumer tag: %s] Starting message processing: %s", consumerTag, body));
            msgHandler.onMessage(body);
            LOG.debug(String.format("[consumer tag: %s] Message has been processed: %s. About to send AMQP ack...", consumerTag, body));
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOG.error(String.format("[consumer tag: %s] Failed to process message %s", consumerTag, body), e);
        }
    }
}
