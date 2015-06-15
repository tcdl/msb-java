package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.github.tcdl.exception.ChannelException;
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
 * To address this issue {@link AmqpMsbConsumer} just takes incoming message, wraps it in a task and puts into a thread pool. And the actual
 * processing is happening in the separate thread from that thread pool.
 */
public class AmqpMsbConsumer extends DefaultConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AmqpMsbConsumer.class);

    ExecutorService consumerThreadPool;
    RawMessageHandler msgHandler;

    public AmqpMsbConsumer(Channel channel, ExecutorService consumerThreadPool, RawMessageHandler msgHandler) {
        super(channel);
        this.consumerThreadPool = consumerThreadPool;
        this.msgHandler = msgHandler;
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String bodyStr = new String(body);
        logger.debug(String.format("[consumer tag: %s] Message consumed from broker: %s", consumerTag, bodyStr));

        consumerThreadPool.submit(() -> {
            logger.debug(String.format("[consumer tag: %s] Starting message processing: %s", consumerTag, bodyStr));
            msgHandler.onMessage(bodyStr);
            logger.debug(String.format("[consumer tag: %s] Message has been processed: %s. About to send AMQP ack...", consumerTag, bodyStr));
            try {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                logger.debug(String.format("[consumer tag: %s] AMQP ack has been sent for message: %s", consumerTag, bodyStr));
            } catch (IOException e) {
                throw new ChannelException(String.format("[consumer tag: %s] Failed to ack message %s", consumerTag, bodyStr), e);
            }
        });
    }
}
