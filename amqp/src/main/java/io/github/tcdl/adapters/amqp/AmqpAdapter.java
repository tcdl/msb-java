package io.github.tcdl.adapters.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import io.github.tcdl.adapters.Adapter;
import io.github.tcdl.config.amqp.AmqpBrokerConfig;
import io.github.tcdl.exception.ChannelException;
import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 *  The {@link AmqpAdapter} class implements {@link Adapter}
 *  and encapsulates logic of interaction with RabbitMQ.
 */
public class AmqpAdapter implements Adapter {
    private String topic;

    private Channel channel;
    private String exchangeName;
    private String consumerTag;
    private AmqpBrokerConfig adapterConfig;
    private ExecutorService consumerThreadPool;

    /**
     * The constructor.
     * @param topic - a topic name associated with the adapter
     * @param consumerThreadPool contains incoming messages wrapped as tasks for further processing. Parameters of this thread pool determine degree of
     *                           parallelism of incoming message processing
     */
    public AmqpAdapter(String topic, AmqpBrokerConfig amqpBrokerConfig, AmqpConnectionManager connectionManager, ExecutorService consumerThreadPool) {
        Validate.notNull(topic, "the 'topic' must not be null");

        this.topic = topic;
        this.exchangeName = topic;
        this.adapterConfig = amqpBrokerConfig;
        this.consumerThreadPool = consumerThreadPool;

        try {
            channel = connectionManager.obtainConnection().createChannel();
            channel.exchangeDeclare(exchangeName, "fanout", false /* durable */, true /* auto-delete */, null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to setup channel from ActiveMQ connection", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(String jsonMessage) throws ChannelException {
        try {
            channel.basicPublish(exchangeName, "" /* routing key */, MessageProperties.PERSISTENT_BASIC, jsonMessage.getBytes());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to publish message '%s' into exchange '%s'", jsonMessage, exchangeName), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(RawMessageHandler msgHandler) {
        String groupId = adapterConfig.getGroupId();
        boolean durable = adapterConfig.isDurable();

        String queueName = generateQueueName(topic, groupId, durable);

        try {
            channel.queueDeclare(queueName, durable /* durable */, false /* exclusive */, !durable /*auto-delete */, null);
            channel.queueBind(queueName, exchangeName, "");

            consumerTag = channel.basicConsume(queueName, false /* autoAck */, new AmqpMsbConsumer(channel, consumerThreadPool, msgHandler));
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to subscribe to topic %s", topic), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe() {
        try {
            channel.basicCancel(consumerTag);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to unsubscribe from topic %s", topic), e);
        }
    }

    /**
     * Generate topic name to get unique topics for different microservices
     * @param topic - topic name associated with the adapter
     * @param groupId - group service Id
     * @param durable - queue durability
     */
    private String generateQueueName(String topic, String groupId, boolean durable) {
        return topic + "." + groupId + "." + (durable ? "d" : "t");
    }
    
}
